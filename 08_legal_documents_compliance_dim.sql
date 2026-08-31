-- ============================================================================
-- Legal documents compliance dimension
-- ----------------------------------------------------------------------------
-- Builds a compliance dimension for supplier legal documents (Kbis, RC,
-- Urssaf, Attestation fiscale, Attestation de travail, DPAE) across three
-- assignment sources: RFPs, Quotes and direct POs.
--
-- Business rules for DPAE applicability:
--   - Freelance consultant                             → Not concerned
--   - IT company consultant with subcontracting        → Not concerned
--     (DPAE is held by the subcontractor)
--   - IT company consultant without subcontracting     → DPAE required
--
-- Architectural note:
--   This pipeline mixes raw and processed schemas because `subcontracting_type`
--   is only available in raw tables:
--     * For QUOTE and PO assignments → raw.contract_assignment_settings
--     * For RFP assignments → raw.rfp_application_subcontracting, joined with
--       raw.rfp_applications to deduplicate by (rfp_id, company_id) and take
--       the most recent proposal.
--
--   To keep consistency with these mandatory raw joins, the pipeline also
--   rebuilds from raw the entities that cross them (assignments, quotations,
--   rfps, company_entity, config_region, contract_company, allocations,
--   users, companies, users_encoded). Migrating only part of the pipeline
--   to processed would introduce key misalignments and complicate the
--   mapping with the required raw joins.
--
--   Once the subcontracting tables are exposed in processed, this pipeline
--   can be fully migrated. Until then, a mixed raw + processed approach is
--   used (fact_supplier_legal_doc and dim_supplier_company already come
--   from processed) to reduce joins where possible without compromising
--   the subcontracting logic.
-- ============================================================================

with affectations as (
  select
    ma.*,
    concat(u.first_name, ' ', u.last_name) as name,
    ep.email,
    case
      when u.ghost = 0 and u.destroyed = 0 and u.desactivated_date is null and u.disaffiliated = 0
      then u.mobile_phone
      else null
    end as mobile_phone
  from raw.assignment_allocations as ma
  left join raw.users as u on ma.user_id = u.id
  left join raw.email_preferences as ep
    on u.id = ep.user_id
   and u.ghost = 0
   and u.destroyed = 0
   and u.desactivated_date is null
   and u.disaffiliated = 0
  where ma.ghost = 0
    and ma.start_date >= '2020-01-01'
),

-- Individual consultant lookup (used to resolve DPAE consultant name)
consultant_individual as (
  select distinct
    ma.user_id as consultant_user_id,
    concat(u.first_name, ' ', u.last_name) as consultant_name
  from raw.assignment_allocations as ma
  left join raw.users as u on ma.user_id = u.id
  where ma.type = 'CONSULTANT'
    and ma.ghost = 0
    and ma.start_date >= '2020-01-01'
),

-- Consultant-by-assignment (unfiltered by 2020) to validate DPAE ownership
consultants_by_mission as (
  select distinct
    ma.mission_id,
    ma.user_id as consultant_user_id,
    concat(u.first_name, ' ', u.last_name) as consultant_name
  from raw.assignment_allocations as ma
  left join raw.users as u on ma.user_id = u.id
  where ma.type = 'CONSULTANT'
    and ma.ghost = 0
),

-- Concatenated participants per assignment (sorted for stability)
affectations2 as (
  select
    ma.mission_id,
    concat_ws('; ', sort_array(collect_list(case when type = 'CONSULTANT' then ma.user_id end))) as consultant_id,
    concat_ws('; ', sort_array(collect_list(case when type = 'CONSULTANT' then cast(ma.name as string) end))) as consultant,
    concat_ws('; ', sort_array(collect_list(case when type = 'CONSULTANT' then cast(ma.email as string) end))) as consultant_email,
    concat_ws('; ', sort_array(collect_list(case when type = 'SELLER' then cast(ma.name as string) end))) as seller,
    concat_ws('; ', sort_array(collect_list(case when type = 'SELLER' then cast(ma.email as string) end))) as seller_email
  from affectations as ma
  where ma.ghost = 0
  group by ma.mission_id
),

-- Quotations preprocessing: keep only one row per external_reference prefix
quote_0 as (
  select
    q.*,
    row_number() over (partition by split(q.external_reference, '-')[0] order by q.internal_reference, m.id asc) as rn
  from raw.quotations as q
  left join raw.assignments as m
    on q.internal_reference = m.source_id
    and m.source_type = 'QUOTATION'
),

quote as (
  select
    q.*,
    concat('quote_', q.internal_reference, '_', q.rn) as quote_id,
    split(q.external_reference, '-')[0] as project_id,
    case when m.id is not null then m.project_index else q.rn end as project_index
  from quote_0 as q
  left join raw.assignments as m
    on q.internal_reference = m.source_id
    and q.rn = m.project_index
    and m.source_type = 'QUOTATION'
),

-- Unified assignment ID across the three source types (RFP, QUOTE, PO)
merge_id as (
  select concat('rfp_', r.id) as id, r.project_id
  from raw.rfps as r
  where (case when r.published_at is null and year(r.start_date) >= 2020
              then year(current_date) else year(r.published_at) end) >= 2020
  union all
  select q.quote_id as id, q.project_id
  from quote as q
  union all
  select concat('po_', m.id) as id, m.project_id
  from raw.assignments as m
  where m.source_type = 'PROJECT'
    and year(m.start_date) >= 2020
),

supplier_type as (
  select
    ma.mission_id,
    u.account_type,
    row_number() over (partition by ma.mission_id order by ma.id desc) as rn
  from raw.assignment_allocations as ma
  left join raw.users as u on ma.user_id = u.id
  where ma.type = 'CONSULTANT'
    and ma.ghost = 0
),

-- Deduplicate RFP subcontracting to latest proposal per (rfp, supplier)
rfp_subcontracting as (
  select
    ra.rfp_id,
    ra.company_id,
    ra.id,
    ras.type,
    row_number() over (
      partition by ra.rfp_id, ra.company_id
      order by ras.proposal_id desc nulls last
    ) as rn
  from raw.rfp_applications as ra
  left join raw.rfp_application_subcontracting as ras
    on ra.id = ras.proposal_id
  where ra.canceled_at is null
    and ra.ghost = 0
),

-- Assignment dimension unified across all three sources
mission2 as (
  select
    mid.id as po_sk,
    case
      when mid.id like 'rfp_%' then 'RFP'
      when mid.id like 'quote_%' then 'QUOTE'
      when mid.id like 'po_%' then 'PO'
    end as source_type,
    case
      when mid.id like 'rfp_%' then m_rfp.id
      when mid.id like 'quote_%' then m_quote.id
      when mid.id like 'po_%' then m_po.id
    end as po_id,
    mid.project_id,
    case
      when mid.id like 'rfp_%' then coalesce(m_rfp.presta_company_id, m_prev.presta_company_id)
      when mid.id like 'quote_%' then m_quote.presta_company_id
      when mid.id like 'po_%' then m_po.presta_company_id
    end as supplier_id,
    case
      when mid.id like 'rfp_%' then cr_rfp.country_code
      when mid.id like 'quote_%' then cr_quote.country_code
      when mid.id like 'po_%' then cr_po.country_code
    end as country_code
  from merge_id as mid
  left join raw.rfps as r on mid.id = concat('rfp_', r.id)
  left join raw.assignments as m_rfp
    on r.id = m_rfp.source_id and m_rfp.source_type = 'RFP' and m_rfp.ghost = 0
  left join raw.company_entity as ce_rfp on r.entityId = ce_rfp.id
  left join raw.regions as cr_rfp on ce_rfp.config_region_id = cr_rfp.id and cr_rfp.ghost = 0
  left join raw.assignments as m_prev
    on mid.id like 'rfp_%' and m_rfp.id is null
    and m_prev.project_id = r.project_id
    and m_prev.ghost = 0
  left join quote as q on mid.id = q.quote_id
  left join raw.assignments as m_quote
    on q.internal_reference = m_quote.source_id and m_quote.source_type = 'QUOTATION' and m_quote.ghost = 0
  left join raw.contract_company as cc_q on q.client_company_id = cc_q.id
  left join raw.company_entity as ce_quote on cc_q.entity_id = ce_quote.id
  left join raw.regions as cr_quote on ce_quote.config_region_id = cr_quote.id and cr_quote.ghost = 0
  left join raw.assignments as m_po
    on mid.id = concat('po_', m_po.id) and m_po.source_type = 'PROJECT' and m_po.ghost = 0
  left join raw.contract_assignment_settings as cmas_po on m_po.id = cmas_po.mission_id
  left join raw.company_entity as ce_po on cmas_po.client_entity_id = ce_po.id
  left join raw.regions as cr_po on ce_po.config_region_id = cr_po.id and cr_po.ghost = 0
  where (mid.id not like 'rfp_%' or (r.status not in (4,5,10,12) and r.ghost <> 1))
    and (mid.id not like 'quote_%' or q.ghost = false)
    and (mid.id not like 'po_%' or m_po.ghost = false)
),

-- Legal documents from fact table. Non-DPAE documents are unique per
-- (company, file_type). DPAE documents are unique per (company, file_type,
-- consultant) since one supplier can have multiple DPAE, one per consultant.
documents as (
  select
    c.id as company_id, f.legal_doc_sk, f.file_id, f.supplier_company_id,
    f.comments, f.uploaded_at, f.file_name, f.file_type, f.validated,
    f.validated_at, f.validator_id, f.version, f.is_latest_version,
    f.dpae_supplier_encoded_id, f.is_active, f.days_until_expiration,
    f.validity_start_date, f.validity_end_date,
    row_number() over (partition by c.id, f.file_type order by f.uploaded_at desc) as rn
  from raw.companies as c
  left join warehouse.dim_supplier_company as sc
    on sc.file_attached_id = c.attachements_id
  left join warehouse.fact_supplier_legal_doc as f
    on f.supplier_company_id = sc.platform_id
    and f.is_latest_version = true
  where f.file_type in ('Kbis', 'RC', 'Urssaf', 'Attestation fiscale', 'Attestation de travail')

  UNION ALL

  select
    c.id as company_id, f.legal_doc_sk, f.file_id, f.supplier_company_id,
    f.comments, f.uploaded_at, f.file_name, f.file_type, f.validated,
    f.validated_at, f.validator_id, f.version, f.is_latest_version,
    f.dpae_supplier_encoded_id, f.is_active, f.days_until_expiration,
    f.validity_start_date, f.validity_end_date,
    row_number() over (
      partition by c.id, f.file_type, f.dpae_supplier_encoded_id
      order by f.uploaded_at desc
    ) as rn
  from raw.companies as c
  left join warehouse.dim_supplier_company as sc
    on sc.file_attached_id = c.attachements_id
  left join warehouse.fact_supplier_legal_doc as f
    on f.supplier_company_id = sc.platform_id
  where f.file_type = 'DPAE'
),

-- Enriched raw dimension: adds consultant, seller, subcontracting type
dim_legal_documents_raw as (
  select distinct
    d.legal_doc_sk, d.file_id, d.supplier_company_id, m.po_sk as source_id,
    d.comments, d.uploaded_at, d.file_name, d.file_type, d.validated,
    d.validated_at, d.validator_id, d.version, d.is_latest_version,
    d.dpae_supplier_encoded_id,
    ue.user_id as dpae_user_id,
    d.is_active, d.days_until_expiration,
    d.validity_start_date, d.validity_end_date,
    -- Subcontracting resolution: RFP → rfp_applications; QUOTE/PO → contract settings
    case
      when m.source_type = 'RFP' then ras_dedup.type
      else cmas.subcontracting_type
    end as subcontracting_type,
    case
      when d.file_type = 'DPAE' then ci.consultant_name
      else a.consultant
    end as consultant,
    case when st.account_type = 5 then 'Freelance' else 'IT company' end as consultant_type,
    a.seller,
    a.seller_email,
    m.country_code,
    m.source_type
  from mission2 as m
  left join documents as d on m.supplier_id = d.company_id
  left join supplier_type as st on m.po_id = st.mission_id and st.rn = 1
  left join affectations2 as a on m.po_id = a.mission_id
  left join raw.users_encoded as ue
    on d.dpae_supplier_encoded_id = ue.user_encoded
  left join consultant_individual as ci
    on ci.consultant_user_id = ue.user_id
  left join consultants_by_mission as cbm
    on m.po_id = cbm.mission_id
    and ue.user_id = cbm.consultant_user_id
  left join rfp_subcontracting as ras_dedup
    on m.po_sk = concat('rfp_', ras_dedup.rfp_id)
    and m.supplier_id = ras_dedup.company_id
    and ras_dedup.rn = 1
  left join raw.contract_assignment_settings as cmas
    on m.po_id = cmas.mission_id
  where (d.rn = 1 OR d.rn IS NULL)
    -- Guard: some validity dates are stored as '+99999-...' overflows; treat as NULL
    AND (CAST(d.validity_start_date AS STRING) NOT LIKE '+%' OR d.validity_start_date IS NULL)
    AND (CAST(d.validity_end_date AS STRING) NOT LIKE '+%' OR d.validity_end_date IS NULL)
    AND (
      -- For DPAE, keep only rows we can attach to a known consultant on this assignment
      d.file_type <> 'DPAE'
      OR d.dpae_supplier_encoded_id IS NULL
      OR cbm.consultant_user_id IS NOT NULL
    )
),

-- Deduplicate DPAE at (supplier, consultant, source) level
dim_legal_documents as (
  select * from (
    select
      *,
      case
        when file_type = 'DPAE'
        then row_number() over (
          partition by supplier_company_id, dpae_supplier_encoded_id, source_id
          order by uploaded_at desc
        )
        else 1
      end as dpae_rn
    from dim_legal_documents_raw
  ) where dpae_rn = 1
),

-- =============== Per-document status aggregations ===============

statut_dpae as (
  select
    consultant,
    case
      when max(validated_at) is not null then 'Actif'
      when max(uploaded_at) is not null then 'A vérifier'
      else 'Document existant sans date'
    end as DPAE
  from dim_legal_documents
  where file_type = 'DPAE'
  group by consultant
),

statut_attestation_travail as (
  select
    supplier_company_id,
    case
      when max(validity_end_date) is null then 'Absent'
      when max(validity_end_date) < current_date then 'Expiré'
      when max(validity_end_date) <= date_add(current_date, 15) then 'Alerte'
      else 'Actif'
    end as attestation_travail
  from dim_legal_documents
  where file_type = 'Attestation de travail'
  group by supplier_company_id
),

statut_rc as (
  select
    supplier_company_id,
    case
      when max(validity_end_date) is null then 'Absent'
      when max(validity_end_date) < current_date then 'Expiré'
      when max(validity_end_date) <= date_add(current_date, 15) then 'Alerte'
      else 'Actif'
    end as RC
  from dim_legal_documents
  where file_type = 'RC'
  group by supplier_company_id
),

statut_urssaf as (
  select
    supplier_company_id,
    case
      when max(validity_end_date) is null then 'Absent'
      when max(validity_end_date) < current_date then 'Expiré'
      when max(validity_end_date) <= date_add(current_date, 15) then 'Alerte'
      else 'Actif'
    end as Urssaf
  from dim_legal_documents
  where file_type = 'Urssaf'
  group by supplier_company_id
),

statut_attestation_fiscale as (
  select
    supplier_company_id,
    case
      when max(validity_end_date) is null then 'Absent'
      when max(validity_end_date) < current_date then 'Expiré'
      when max(validity_end_date) <= date_add(current_date, 15) then 'Alerte'
      else 'Actif'
    end as attestation_fiscale
  from dim_legal_documents
  where file_type = 'Attestation fiscale'
  group by supplier_company_id
)

-- =============== Final output ===============
select
  dld.legal_doc_sk,
  dld.file_id,
  dld.supplier_company_id,
  dld.source_id,
  dld.source_type,
  dld.comments,
  dld.uploaded_at,
  dld.file_name,
  dld.file_type,
  dld.validated,
  dld.validated_at,
  dld.validator_id,
  dld.version,
  dld.is_latest_version,
  dld.dpae_supplier_encoded_id,
  dld.dpae_user_id,
  dld.is_active,
  dld.days_until_expiration,
  -- Neutralise overflow dates coming from upstream
  CASE
    WHEN year(dld.validity_start_date) < 1900 OR year(dld.validity_start_date) > 9000 THEN NULL
    ELSE dld.validity_start_date
  END as validity_start_date,
  CASE
    WHEN year(dld.validity_end_date) < 1900 OR year(dld.validity_end_date) > 9000 THEN NULL
    ELSE dld.validity_end_date
  END as validity_end_date,
  dld.consultant,
  dld.consultant_type,
  dld.subcontracting_type,
  dld.seller,
  dld.seller_email,
  dld.country_code,
  coalesce(f.attestation_fiscale, 'Absent') as `Attestation fiscale`,
  coalesce(rc.RC, 'Absent') as RC,
  case when dld.consultant_type = 'Freelance' then 'Non concerné'
       else coalesce(t.attestation_travail, 'Absent') end as `Attestation travail`,
  case when dld.consultant_type = 'Freelance' then 'Non concerné'
       else coalesce(u.Urssaf, 'Absent') end as Urssaf,
  -- DPAE applicability rules (see file header)
  case when dld.consultant_type = 'Freelance' then 'Non concerné'
       when dld.subcontracting_type in ('IT Company', 'IT_COMPANY', 'Freelancer', 'SERVICE_COMPANY', 'FREELANCER') then 'Non concerné'
       else coalesce(d.DPAE, 'Absent') end as DPAE,
  -- Notification trigger: 'Yes' if any required document is missing or expired
  case when (coalesce(f.attestation_fiscale, 'Absent') in ('Actif', 'Non concerné'))
     and (coalesce(rc.RC, 'Absent') in ('Actif', 'Non concerné'))
     and (case when dld.consultant_type = 'Freelance' then 'Non concerné'
               else coalesce(t.attestation_travail, 'Absent') end in ('Actif', 'Non concerné'))
     and (case when dld.consultant_type = 'Freelance' then 'Non concerné'
               else coalesce(u.Urssaf, 'Absent') end in ('Actif', 'Non concerné'))
     and (case when dld.consultant_type = 'Freelance' then 'Non concerné'
               when dld.subcontracting_type in ('IT Company', 'IT_COMPANY', 'Freelancer', 'SERVICE_COMPANY', 'FREELANCER') then 'Non concerné'
               else coalesce(d.DPAE, 'Absent') end in ('Actif', 'Non concerné'))
    then 'No'
    else 'Yes'
  end as `To be notified?`
from dim_legal_documents as dld
left join statut_attestation_fiscale f on dld.supplier_company_id = f.supplier_company_id
left join statut_attestation_travail t on dld.supplier_company_id = t.supplier_company_id
left join statut_rc rc on dld.supplier_company_id = rc.supplier_company_id
left join statut_urssaf u on dld.supplier_company_id = u.supplier_company_id
left join statut_dpae d on dld.consultant = d.consultant
