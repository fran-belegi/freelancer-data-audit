// ============================================================================
// Activity report & invoicing ageing (Power Query M)
// ----------------------------------------------------------------------------
// Joins client-side invoices with monthly activity reports (AR) to produce a
// per-reporting-date ageing view.
//
// Data pipeline:
//   1. `ar` CTE — flattens the activity report validation workflow and
//      computes AR amounts (main mission, OCD, regulation) with proper sign
//      convention, net and gross.
//   2. `inv` CTE — same for client invoices; adjusts sign for credit notes.
//   3. `spine` CTE — full outer join between AR and INV on (po_sk, year,
//      month) so that an AR without INV, or an INV without AR, still appears
//      (built as UNION ALL of left join + left anti join).
//   4. `fact` CTE — enriches the spine with project attributes.
//
// The M steps below expand each row into per-Friday reporting dates from the
// AR service month up to invoice validation (or today), then flag pending AR
// / pending INV states and bucket ageing.
// ============================================================================
let
    Source = Query1(
        "WITH#(lf)" &
        "ar_components AS (#(lf)" &
        "    SELECT#(lf)" &
        "      ar.ar_id,#(lf)" &
        "      ar.ar_business_id,#(lf)" &
        "      ar.po_sk,#(lf)" &
        "      ar.client_id,#(lf)" &
        "      ar.legal_entity_id,#(lf)" &
        "      ar.year,#(lf)" &
        "      ar.month,#(lf)" &
        "      ar.currency,#(lf)" &
        "      ar.generate_invoice,#(lf)" &
        "      ar.ar_provision_id,#(lf)" &
        "      vw.status                                        AS ar_last_status,#(lf)" &
        "      vw.ar_validation_date                            AS ar_last_validation_date,#(lf)" &
        "      vw.ar_validated                                  AS ar_validated,#(lf)" &
        "      COALESCE(ar.ar_amount.total_working_days, 0)     AS working_days,#(lf)" &
        "      COALESCE(ar.ar_amount.total_worked_units, 0)     AS worked_unit,#(lf)" &
        "      ar.ar_amount.work_unit_type                      AS work_unit_type,#(lf)" &
        "      -- main mission#(lf)" &
        "      CASE#(lf)" &
        "        WHEN ar.ar_amount.work_unit_type = 'Package' THEN 0#(lf)" &
        "        WHEN ar.ar_amount.work_unit_type = 'Month'   THEN COALESCE(ar.ar_amount.client_price, 0)#(lf)" &
        "        ELSE ROUND(COALESCE(ar.ar_amount.client_price, 0) * COALESCE(ar.ar_amount.total_worked_units, 0), 2)#(lf)" &
        "      END AS ar_amount_without_vat,#(lf)" &
        "      CASE#(lf)" &
        "        WHEN ar.ar_amount.work_unit_type = 'Package' THEN 0#(lf)" &
        "        WHEN ar.ar_amount.work_unit_type = 'Month'   THEN#(lf)" &
        "          ROUND(COALESCE(ar.ar_amount.client_price, 0) * (1 + COALESCE(ar.ar_amount.client_tax_percentage, 0) / 100), 2)#(lf)" &
        "        ELSE#(lf)" &
        "          ROUND((COALESCE(ar.ar_amount.client_price, 0) * COALESCE(ar.ar_amount.total_worked_units, 0))#(lf)" &
        "                * (1 + COALESCE(ar.ar_amount.client_tax_percentage, 0) / 100), 2)#(lf)" &
        "      END AS ar_amount_with_vat,#(lf)" &
        "      -- OCD (on-call duty)#(lf)" &
        "      COALESCE(ar.ar_ocd.price, 0) AS ar_ocd_without_vat,#(lf)" &
        "      ROUND(COALESCE(ar.ar_ocd.price, 0) * (1 + COALESCE(ar.ar_ocd.client_tax_percentage, 0) / 100), 2) AS ar_ocd_with_vat,#(lf)" &
        "      -- Regulation (fallback to units-based if price is 0/null)#(lf)" &
        "      CASE#(lf)" &
        "        WHEN COALESCE(ar.ar_regulation.price, 0) = 0#(lf)" &
        "          THEN ROUND(COALESCE(ar.ar_amount.client_price, 0) * COALESCE(ar.ar_regulation.regulation_days, 0), 2)#(lf)" &
        "        ELSE COALESCE(ar.ar_regulation.price, 0)#(lf)" &
        "      END AS ar_regulation_without_vat,#(lf)" &
        "      COALESCE(#(lf)" &
        "        ar.ar_regulation.price + ROUND((ar.ar_regulation.price * (ar.ar_regulation.client_tax_percentage / 100)), 2),#(lf)" &
        "        0#(lf)" &
        "      ) AS ar_regulation_with_vat#(lf)" &
        "    FROM warehouse.dim_activity_report AS ar#(lf)" &
        "    LATERAL VIEW OUTER EXPLODE(ar.validation_workflow) AS vw#(lf)" &
        "    WHERE vw.current_validation = TRUE OR ar.validation_workflow IS NULL#(lf)" &
        "),#(lf)" &
        "ar AS (#(lf)" &
        "    SELECT#(lf)" &
        "      *,#(lf)" &
        "      ROUND(COALESCE(ar_amount_without_vat, 0) + ar_ocd_without_vat + ar_regulation_without_vat, 2) AS ar_amount_without_vat_total,#(lf)" &
        "      ROUND(COALESCE(ar_amount_with_vat, 0)    + ar_ocd_with_vat    + ar_regulation_with_vat,    2) AS ar_amount_with_vat_total#(lf)" &
        "    FROM ar_components#(lf)" &
        "),#(lf)" &
        "inv_components AS (#(lf)" &
        "    SELECT#(lf)" &
        "      i.inv_id, i.inv_business_id, i.inv_type, i.po_sk,#(lf)" &
        "      i.final_client_id, i.legal_entity_id, i.year, i.month, i.currency,#(lf)" &
        "      i.inv_status, i.inv_created_at, i.inv_emission_date, i.inv_validated_at,#(lf)" &
        "      i.inv_billing_due_date_adjusted               AS inv_billing_due_date,#(lf)" &
        "      i.inv_billing_date_nb_days, i.inv_billing_type,#(lf)" &
        "      i.is_expenses, i.is_complementary, i.is_overtime, i.is_client, i.is_cancel_with_credit_note,#(lf)" &
        "      i.amount.work_unit_type                       AS inv_work_unit_type,#(lf)" &
        "      CASE#(lf)" &
        "        WHEN i.inv_type = 'Invoice Complementary C' THEN#(lf)" &
        "          COALESCE(AGGREGATE(#(lf)" &
        "              ZIP_WITH(i.product_service.price, i.product_service.quantity, (p2, q) -> COALESCE(p2, 0) * COALESCE(q, 0)),#(lf)" &
        "              CAST(0.0 AS DOUBLE),#(lf)" &
        "              (acc, x) -> acc + x#(lf)          ), 0)#(lf)" &
        "        ELSE COALESCE(i.amount.price * i.amount.quantity, 0)#(lf)" &
        "      END AS raw_inv_amount_net,#(lf)" &
        "      CASE#(lf)" &
        "        WHEN i.inv_type = 'Invoice Complementary C' THEN#(lf)" &
        "          ROUND(COALESCE(AGGREGATE(#(lf)" &
        "                ZIP_WITH(#(lf)" &
        "                  ZIP_WITH(i.product_service.price, i.product_service.quantity, (p2, q) -> COALESCE(p2, 0) * COALESCE(q, 0)),#(lf)" &
        "                  i.product_service.tax_percentage,#(lf)" &
        "                  (subtotal, tax_array) -> subtotal * (1 + COALESCE(#(lf)" &
        "                    AGGREGATE(tax_array, CAST(0.0 AS DOUBLE), (acc, t) -> acc + COALESCE(TRY_CAST(t AS DOUBLE), 0.0)),#(lf)" &
        "                    0.0) / 100)#(lf)                ),#(lf)" &
        "                CAST(0.0 AS DOUBLE),#(lf)" &
        "                (acc, x) -> acc + x#(lf)              ), 0),#(lf)          2)#(lf)" &
        "        ELSE#(lf)" &
        "          ROUND(COALESCE(i.amount.price * i.amount.quantity, 0)#(lf)" &
        "            * (1 + COALESCE(AGGREGATE(i.amount.tax, CAST(0.0 AS DOUBLE),#(lf)" &
        "                 (acc, t) -> acc + COALESCE(TRY_CAST(t.tax_percentage AS DOUBLE), 0.0)), 0.0) / 100),#(lf)          2)#(lf)" &
        "      END AS raw_inv_amount_gross,#(lf)" &
        "      COALESCE(i.ocd.price * i.ocd.quantity, 0) AS raw_inv_ocd_net,#(lf)" &
        "      ROUND(COALESCE(i.ocd.price * i.ocd.quantity, 0)#(lf)" &
        "        * (1 + COALESCE(AGGREGATE(i.ocd.tax_percentage, CAST(0.0 AS DOUBLE),#(lf)" &
        "             (acc, t) -> acc + COALESCE(TRY_CAST(t AS DOUBLE), 0.0)), 0.0) / 100),#(lf)      2) AS raw_inv_ocd_gross,#(lf)" &
        "      COALESCE(i.regulation.price * i.regulation.quantity, 0) AS raw_inv_reg_net,#(lf)" &
        "      ROUND(COALESCE(i.regulation.price * i.regulation.quantity, 0)#(lf)" &
        "        * (1 + COALESCE(AGGREGATE(i.regulation.tax_percentage, CAST(0.0 AS DOUBLE),#(lf)" &
        "             (acc, t) -> acc + COALESCE(TRY_CAST(t AS DOUBLE), 0.0)), 0.0) / 100),#(lf)      2) AS raw_inv_reg_gross#(lf)" &
        "    FROM warehouse.dim_invoice AS i#(lf)" &
        "    WHERE i.is_client = TRUE#(lf)" &
        "),#(lf)" &
        "inv AS (#(lf)" &
        "    SELECT#(lf)" &
        "      *,#(lf)" &
        "      CASE#(lf)" &
        "        WHEN inv_type IN ('Invoice C', 'Invoice Complementary C') THEN  ABS(raw_inv_amount_net)#(lf)" &
        "        WHEN inv_type = 'Credit Note N'                           THEN -ABS(raw_inv_amount_net)#(lf)" &
        "        ELSE raw_inv_amount_net#(lf)" &
        "      END AS inv_amount_without_vat,#(lf)" &
        "      CASE#(lf)" &
        "        WHEN inv_type IN ('Invoice C', 'Invoice Complementary C') THEN  ABS(raw_inv_amount_gross)#(lf)" &
        "        WHEN inv_type = 'Credit Note N'                           THEN -ABS(raw_inv_amount_gross)#(lf)" &
        "        ELSE raw_inv_amount_gross#(lf)" &
        "      END AS inv_amount_with_vat,#(lf)" &
        "      CASE#(lf)" &
        "        WHEN inv_type = 'Invoice C'     THEN  ABS(raw_inv_ocd_net)#(lf)" &
        "        WHEN inv_type = 'Credit Note N' THEN -ABS(raw_inv_ocd_net)#(lf)" &
        "        ELSE raw_inv_ocd_net#(lf)" &
        "      END AS inv_ocd_without_vat,#(lf)" &
        "      CASE#(lf)" &
        "        WHEN inv_type = 'Invoice C'     THEN  ABS(raw_inv_ocd_gross)#(lf)" &
        "        WHEN inv_type = 'Credit Note N' THEN -ABS(raw_inv_ocd_gross)#(lf)" &
        "        ELSE raw_inv_ocd_gross#(lf)" &
        "      END AS inv_ocd_with_vat,#(lf)" &
        "      CASE#(lf)" &
        "        WHEN inv_type = 'Invoice C'     THEN  ABS(raw_inv_reg_net)#(lf)" &
        "        WHEN inv_type = 'Credit Note N' THEN -ABS(raw_inv_reg_net)#(lf)" &
        "        ELSE raw_inv_reg_net#(lf)" &
        "      END AS inv_regulation_without_vat,#(lf)" &
        "      CASE#(lf)" &
        "        WHEN inv_type = 'Invoice C'     THEN  ABS(raw_inv_reg_gross)#(lf)" &
        "        WHEN inv_type = 'Credit Note N' THEN -ABS(raw_inv_reg_gross)#(lf)" &
        "        ELSE raw_inv_reg_gross#(lf)" &
        "      END AS inv_regulation_with_vat#(lf)" &
        "    FROM inv_components#(lf)" &
        "),#(lf)" &
        "-- Full outer spine of (AR × INV) on (po_sk, year, month) via UNION ALL#(lf)" &
        "spine AS (#(lf)" &
        "    SELECT ar.ar_id, inv.inv_id, ar.po_sk, ar.year, ar.month#(lf)" &
        "    FROM ar#(lf)" &
        "    LEFT JOIN inv ON ar.po_sk = inv.po_sk AND ar.year = inv.year AND ar.month = inv.month#(lf)" &
        "    UNION ALL#(lf)" &
        "    SELECT NULL AS ar_id, inv.inv_id, inv.po_sk, inv.year, inv.month#(lf)" &
        "    FROM inv#(lf)" &
        "    LEFT ANTI JOIN ar ON ar.po_sk = inv.po_sk AND ar.year = inv.year AND ar.month = inv.month#(lf)" &
        "),#(lf)" &
        "fact AS (#(lf)" &
        "    SELECT DISTINCT#(lf)" &
        "      s.ar_id, a.ar_business_id, a.ar_provision_id,#(lf)" &
        "      p.project_id, s.po_sk, p.po_category, p.adjusted_end_date,#(lf)" &
        "      COALESCE(a.client_id, i.final_client_id) AS final_client_id,#(lf)" &
        "      CASE WHEN s.inv_id IS NOT NULL THEN i.legal_entity_id ELSE a.legal_entity_id END AS legal_entity_id,#(lf)" &
        "      'Client' AS client_type,#(lf)" &
        "      make_date(s.year, s.month, 1) AS service_month,#(lf)" &
        "      a.working_days, a.worked_unit,#(lf)" &
        "      COALESCE(a.work_unit_type, i.inv_work_unit_type) AS work_unit_type,#(lf)" &
        "      COALESCE(p.po_validation_date, make_date(s.year, s.month, 1)) AS po_validation_date,#(lf)" &
        "      a.ar_last_validation_date, a.ar_validated,#(lf)" &
        "      s.inv_id, i.inv_business_id, i.inv_type,#(lf)" &
        "      i.inv_created_at, i.inv_emission_date, i.inv_validated_at,#(lf)" &
        "      i.inv_billing_due_date, i.inv_billing_date_nb_days, i.inv_billing_type,#(lf)" &
        "      i.inv_status, i.inv_amount_without_vat, i.inv_amount_with_vat,#(lf)" &
        "      -- Fallback to AR only when no invoice exists#(lf)" &
        "      CASE WHEN s.inv_id IS NULL THEN a.ar_ocd_without_vat        ELSE i.inv_ocd_without_vat        END AS inv_ocd_without_vat,#(lf)" &
        "      CASE WHEN s.inv_id IS NULL THEN a.ar_ocd_with_vat           ELSE i.inv_ocd_with_vat           END AS inv_ocd_with_vat,#(lf)" &
        "      CASE WHEN s.inv_id IS NULL THEN a.ar_regulation_without_vat ELSE i.inv_regulation_without_vat END AS inv_regulation_without_vat,#(lf)" &
        "      CASE WHEN s.inv_id IS NULL THEN a.ar_regulation_with_vat    ELSE i.inv_regulation_with_vat    END AS inv_regulation_with_vat,#(lf)" &
        "      ROUND(a.ar_amount_without_vat, 2) AS ar_amount_without_vat,#(lf)" &
        "      ROUND(a.ar_amount_with_vat, 2)    AS ar_amount_with_vat,#(lf)" &
        "      ROUND(a.ar_ocd_without_vat, 2)    AS ar_ocd_without_vat,#(lf)" &
        "      ROUND(a.ar_ocd_with_vat, 2)       AS ar_ocd_with_vat,#(lf)" &
        "      a.ar_amount_without_vat_total, a.ar_amount_with_vat_total,#(lf)" &
        "      COALESCE(i.currency, a.currency) AS currency,#(lf)" &
        "      i.is_expenses, i.is_complementary, i.is_overtime, i.is_client,#(lf)" &
        "      COALESCE(i.is_cancel_with_credit_note, FALSE) AS cancel_with_credit_note#(lf)" &
        "    FROM spine AS s#(lf)" &
        "    LEFT JOIN ar  AS a ON s.ar_id  = a.ar_id#(lf)" &
        "    LEFT JOIN inv AS i ON s.inv_id = i.inv_id#(lf)" &
        "    LEFT JOIN (#(lf)" &
        "        SELECT po_sk, project_id, po_category, po_validation_date, adjusted_end_date#(lf)" &
        "        FROM warehouse.dim_project WHERE is_valid = TRUE#(lf)" &
        "    ) AS p ON s.po_sk = p.po_sk),#(lf)" &
        "min_validated AS (#(lf)" &
        "    SELECT po_sk, final_client_id, service_month, MIN(inv_validated_at) AS min_inv_validated_at#(lf)" &
        "    FROM fact GROUP BY po_sk, final_client_id, service_month#(lf)" &
        ")#(lf)" &
        "SELECT DISTINCT#(lf)" &
        "  f.ar_id, f.ar_business_id, f.project_id, f.po_sk, f.po_category, f.adjusted_end_date,#(lf)" &
        "  f.final_client_id, f.legal_entity_id, f.client_type,#(lf)" &
        "  f.service_month, f.working_days, f.worked_unit, f.work_unit_type,#(lf)" &
        "  f.po_validation_date, f.ar_last_validation_date, f.ar_validated,#(lf)" &
        "  f.inv_id,#(lf)" &
        "  CASE WHEN f.inv_id IS NULL THEN f.ar_provision_id ELSE f.inv_business_id END AS inv_business_id,#(lf)" &
        "  f.inv_type, f.inv_created_at, f.inv_emission_date, f.inv_validated_at, mv.min_inv_validated_at,#(lf)" &
        "  f.inv_billing_due_date, f.inv_billing_date_nb_days, f.inv_billing_type, f.inv_status,#(lf)" &
        "  CASE WHEN f.inv_id IS NULL THEN NULL ELSE f.inv_amount_without_vat     END AS inv_amount_without_vat,#(lf)" &
        "  CASE WHEN f.inv_id IS NULL THEN NULL ELSE f.inv_amount_with_vat        END AS inv_amount_with_vat,#(lf)" &
        "  CASE WHEN f.inv_id IS NULL THEN NULL ELSE f.inv_ocd_without_vat        END AS inv_ocd_without_vat,#(lf)" &
        "  CASE WHEN f.inv_id IS NULL THEN NULL ELSE f.inv_ocd_with_vat           END AS inv_ocd_with_vat,#(lf)" &
        "  CASE WHEN f.inv_id IS NULL THEN NULL ELSE f.inv_regulation_without_vat END AS inv_regulation_without_vat,#(lf)" &
        "  CASE WHEN f.inv_id IS NULL THEN NULL ELSE f.inv_regulation_with_vat    END AS inv_regulation_with_vat,#(lf)" &
        "  CASE WHEN f.inv_id IS NULL THEN NULL#(lf)" &
        "       ELSE ROUND(COALESCE(f.inv_amount_without_vat, 0) + COALESCE(f.inv_ocd_without_vat, 0)#(lf)" &
        "                + COALESCE(f.inv_regulation_without_vat, 0), 2)#(lf)" &
        "  END AS inv_amount_without_vat_total,#(lf)" &
        "  CASE WHEN f.inv_id IS NULL THEN NULL#(lf)" &
        "       ELSE ROUND(COALESCE(f.inv_amount_with_vat, 0) + COALESCE(f.inv_ocd_with_vat, 0)#(lf)" &
        "                + COALESCE(f.inv_regulation_with_vat, 0), 2)#(lf)" &
        "  END AS inv_amount_with_vat_total,#(lf)" &
        "  -- AR amounts: Package/expenses/overtime → NULL (kept for compatibility)#(lf)" &
        "  CASE WHEN f.work_unit_type = 'Package' THEN NULL#(lf)" &
        "       WHEN f.is_expenses = TRUE THEN NULL#(lf)" &
        "       WHEN f.is_overtime = TRUE THEN NULL#(lf)" &
        "       WHEN f.inv_type = 'Invoice C' THEN ABS(f.ar_amount_without_vat)#(lf)" &
        "       ELSE f.ar_amount_without_vat END AS ar_amount_without_vat,#(lf)" &
        "  CASE WHEN f.work_unit_type = 'Package' THEN NULL#(lf)" &
        "       WHEN f.is_expenses = TRUE THEN NULL#(lf)" &
        "       WHEN f.is_overtime = TRUE THEN NULL#(lf)" &
        "       WHEN f.inv_type = 'Invoice C' THEN ABS(f.ar_amount_with_vat)#(lf)" &
        "       ELSE f.ar_amount_with_vat END AS ar_amount_with_vat,#(lf)" &
        "  CASE WHEN f.work_unit_type = 'Package' THEN NULL#(lf)" &
        "       WHEN f.is_expenses = TRUE THEN NULL#(lf)" &
        "       WHEN f.is_overtime = TRUE THEN f.inv_ocd_without_vat#(lf)" &
        "       WHEN f.inv_type = 'Invoice C' THEN ABS(f.ar_amount_without_vat_total)#(lf)" &
        "       ELSE f.ar_amount_without_vat_total END AS ar_amount_without_vat_total,#(lf)" &
        "  CASE WHEN f.work_unit_type = 'Package' THEN NULL#(lf)" &
        "       WHEN f.is_expenses = TRUE THEN NULL#(lf)" &
        "       WHEN f.is_overtime = TRUE THEN f.inv_ocd_with_vat#(lf)" &
        "       WHEN f.inv_type = 'Invoice C' THEN ABS(f.ar_amount_with_vat_total)#(lf)" &
        "       ELSE f.ar_amount_with_vat_total END AS ar_amount_with_vat_total,#(lf)" &
        "  f.currency, f.is_expenses, f.is_complementary, f.is_overtime, f.is_client, f.cancel_with_credit_note#(lf)" &
        "FROM fact AS f#(lf)" &
        "LEFT JOIN min_validated AS mv#(lf)" &
        "  ON  f.po_sk = mv.po_sk AND f.service_month = mv.service_month AND f.final_client_id <=> mv.final_client_id#(lf)" &
        "WHERE f.cancel_with_credit_note = FALSE#(lf)" &
        "  AND (f.inv_type = 'Invoice C' OR f.inv_type IS NULL)#(lf)" &
        "  AND YEAR(f.service_month) >= 2023#(lf)" &
        "ORDER BY f.service_month, f.po_sk, f.ar_id"
    ),

    #"Changed Type2" = Table.TransformColumnTypes(Source, {
        {"po_validation_date", type date}, {"service_month", type date},
        {"inv_validated_at", type date}, {"min_inv_validated_at", type date},
        {"adjusted_end_date", type date}, {"inv_created_at", type date},
        {"inv_emission_date", type date}, {"inv_billing_due_date", type date},
        {"ar_last_validation_date", type date},
        {"ar_amount_without_vat", type number}, {"ar_amount_with_vat", type number},
        {"inv_amount_without_vat", type number}, {"inv_amount_with_vat", type number},
        {"inv_amount_without_vat_total", type number}, {"inv_amount_with_vat_total", type number},
        {"inv_ocd_without_vat", type number}, {"inv_ocd_with_vat", type number},
        {"inv_regulation_without_vat", type number}, {"inv_regulation_with_vat", type number}
    }),

    #"Exclude expenses"     = Table.SelectRows(#"Changed Type2",       each [is_expenses] = false or [is_expenses] = null),
    #"Exclude complementary"= Table.SelectRows(#"Exclude expenses",    each [is_complementary] = false or [is_complementary] = null),

    // End of the ageing window: invoice validation date (if validated) or today
    #"Add End Date" = Table.AddColumn(#"Exclude complementary", "end_date", each
        if [inv_status] = "Validated"
        then Date.From([min_inv_validated_at])
        else Date.From(DateTime.LocalNow())
    ),
    // Anchor start: end of the service month
    #"Add Start Date" = Table.AddColumn(#"Add End Date", "start_date", each
        if [service_month] <> null then Date.EndOfMonth([service_month]) else null
    ),
    // Adjust start to PO validation date if it is later (contract not yet signed at service month end)
    #"Add Adjusted Start Date" = Table.AddColumn(#"Add Start Date", "adjusted_start_date", each
        if [po_validation_date] <> null and [service_month] <> null then
            if [po_validation_date] > Date.EndOfMonth([service_month]) then [po_validation_date]
            else Date.EndOfMonth([service_month])
        else null
    ),

    #"Changed Type" = Table.TransformColumnTypes(#"Add Adjusted Start Date", {
        {"end_date", type date}, {"start_date", type date}, {"adjusted_start_date", type date}
    }),

    // Build the list of reporting dates (all Fridays + today + tomorrow + anchors)
    #"Add Date List" = Table.AddColumn(#"Changed Type", "date_list", each
        let
            finalList =
                if [adjusted_start_date] = null or [end_date] = null then null
                else
                let
                    count = Duration.Days([end_date] - [adjusted_start_date]) + 1,
                    safeCount = if count > 0 then count else 0,
                    dateRange = if safeCount > 0
                                then List.Dates([adjusted_start_date], safeCount, #duration(1, 0, 0, 0))
                                else {},
                    fridayList = List.Select(dateRange, each Date.DayOfWeek(_, Day.Monday) = 4),
                    currentDate = Date.From(DateTime.LocalNow()),
                    withCurrentDate = if currentDate >= [adjusted_start_date] and currentDate <= [end_date] and not List.Contains(fridayList, currentDate)
                                      then List.Combine({fridayList, {currentDate}})
                                      else fridayList,
                    tomorrowDate = Date.AddDays(currentDate, 1),
                    withTomorrow = if tomorrowDate >= [adjusted_start_date] and tomorrowDate <= [end_date] and not List.Contains(withCurrentDate, tomorrowDate)
                                   then List.Combine({withCurrentDate, {tomorrowDate}})
                                   else withCurrentDate,
                    withEndDate = if [adjusted_start_date] = [end_date] and not List.Contains(withTomorrow, [end_date])
                                  then List.Combine({withTomorrow, {[end_date]}})
                                  else withTomorrow,
                    noFridayFix = if List.Count(fridayList) = 0
                                  then List.Combine({withEndDate, {[adjusted_start_date], [end_date]}})
                                  else withEndDate,
                    ensureStartDate = if not List.Contains(noFridayFix, [adjusted_start_date])
                                      then List.Combine({noFridayFix, {[adjusted_start_date]}})
                                      else noFridayFix,
                    finalResult = if [adjusted_start_date] > [end_date] and List.Contains(ensureStartDate, [end_date])
                                  then List.RemoveItems(ensureStartDate, {[end_date]})
                                  else ensureStartDate
                in
                    List.Sort(List.Distinct(finalResult), Order.Ascending)
        in
            finalList
    ),

    #"Expanded Date List" = Table.ExpandListColumn(#"Add Date List", "date_list"),
    #"Changed Type1" = Table.TransformColumnTypes(#"Expanded Date List", {{"date_list", type date}}),

    // Cap: forward 12 months max, backward to 2023-01-01
    #"Filter Dates" = Table.SelectRows(#"Changed Type1", each
        [date_list] <= Date.AddMonths(Date.From(DateTime.LocalNow()), 12)
        and [date_list] >= #date(2023, 1, 1)
    ),

    // Flag: AR pending at this reporting date
    #"Add Pending AR" = Table.AddColumn(#"Filter Dates", "pending_ar", each
        if [ar_validated] = false then 1
        else if [ar_validated] = true and [date_list] <> null and [ar_last_validation_date] <> null and [date_list] < [ar_last_validation_date] then 1
        else if [ar_validated] = null and [date_list] > [start_date] then 1
        else 0
    ),
    // Flag: invoice pending at this reporting date (only if AR is not pending)
    #"Add Pending INV" = Table.AddColumn(#"Add Pending AR", "pending_inv", each
        if [pending_ar] = 0 and [inv_status] = "Validated" and [date_list] <> null and [min_inv_validated_at] <> null and [date_list] < [min_inv_validated_at] then 1
        else if [inv_status] = "Pending" then 1
        else if [inv_status] = "To be treated" then 1
        else 0
    ),

    #"Changed Type3" = Table.TransformColumnTypes(#"Add Pending INV", {{"pending_ar", Int64.Type}, {"pending_inv", Int64.Type}}),

    #"Add AR INV Status" = Table.AddColumn(#"Changed Type3", "ar_inv_status", each
        if [pending_ar] = 1 then "Pending AR"
        else if [pending_inv] = 1 then "Pending INV"
        else null
    ),

    // Ageing in months from service_month to reporting date
    #"Add UR Ageing" = Table.AddColumn(#"Add AR INV Status", "ur_ageing", each
        if ([pending_ar] = 1 or [pending_inv] = 1) and [date_list] <> null and [service_month] <> null then
            Number.RoundDown((Date.Year([date_list]) - Date.Year([service_month])) * 12 + (Date.Month([date_list]) - Date.Month([service_month])))
        else null
    ),

    #"Add UR Ageing ID" = Table.AddColumn(#"Add UR Ageing", "ur_ageing_id", each
        if [ur_ageing] = null then null
        else if [ur_ageing] <= 0 then 0
        else if [ur_ageing] = 1 then 1
        else if [ur_ageing] >= 2 and [ur_ageing] <= 3 then 2
        else if [ur_ageing] >= 4 and [ur_ageing] <= 6 then 3
        else if [ur_ageing] >= 7 and [ur_ageing] <= 9 then 4
        else if [ur_ageing] >= 10 and [ur_ageing] <= 12 then 5
        else if [ur_ageing] > 12 then 6
        else null
    ),

    #"Add is Friday" = Table.AddColumn(#"Add UR Ageing ID", "is_friday", each
        if Date.DayOfWeek([date_list], Day.Monday) = 4 or [date_list] = Date.From(DateTime.LocalNow()) then 1 else 0,
        Int64.Type
    ),

    #"Changed Type4" = Table.TransformColumnTypes(#"Add is Friday", {
        {"is_friday", Int64.Type}, {"ur_ageing_id", Int64.Type},
        {"ur_ageing", Int64.Type}, {"ar_inv_status", type text}
    }),

    // Currency and entity enrichment
    #"Merged Dim Entity" = Table.NestedJoin(#"Changed Type4", {"legal_entity_id"}, #"entity", {"entity_id"}, "entity", JoinKind.LeftOuter),
    #"Expanded Dim Entity" = Table.ExpandTableColumn(#"Merged Dim Entity", "entity", {"payment_center_adjusted"}, {"payment_center_adjusted"}),
    #"Add Currency Date" = Table.AddColumn(#"Expanded Dim Entity", "currency_date",
        each if [service_month] < DateTime.Date(DateTime.LocalNow()) then [service_month] else Date.AddDays(DateTime.Date(DateTime.LocalNow()), -1)
    ),
    #"Merged dim currency" = Table.NestedJoin(#"Add Currency Date", {"currency", "currency_date"}, #"currency dim", {"currency", "date"}, "currency dim", JoinKind.LeftOuter),
    #"Expanded Dim Currency" = Table.ExpandTableColumn(#"Merged dim currency", "currency dim", {"daily_rate", "monthly_rate"}, {"daily_rate", "monthly_rate"}),
    #"Add AR VAT EUR" = Table.AddColumn(#"Expanded Dim Currency", "ar_amount_with_vat_eur",
        each Number.Round([ar_amount_with_vat] / [monthly_rate], 2)
    ),
    #"Add AR EUR" = Table.AddColumn(#"Add AR VAT EUR", "ar_amount_without_vat_eur",
        each Number.Round([ar_amount_without_vat] / [monthly_rate], 2)
    ),
    #"Changed Type5" = Table.TransformColumnTypes(#"Add AR EUR", {
        {"ar_amount_with_vat_eur", type number}, {"ar_amount_without_vat_eur", type number}
    })
in
    #"Changed Type5"
