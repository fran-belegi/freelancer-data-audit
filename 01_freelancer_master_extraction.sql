/* ============================================================================
   PROJECT: ERP & Freelancer Portal Reconciliation Framework
   SCRIPT: 01_freelancer_master_extraction.sql
   
   DESCRIPTION: 
   This script builds the foundation for the financial reconciliation process. 
   It extracts core freelancer data from the Data Warehouse and calculates 
   status flags (e.g., Activity logged, Invoices generated, ERP Supplier status) 
   using Common Table Expressions (CTEs) to ensure a 1:1 grain per freelancer 
   and prevent cartesian fan-outs in the final output.
   ============================================================================ */

WITH 
-- 1. CAPTURE THE LATEST VALID AGREEMENT
LatestAgreement AS (
    SELECT 
        [FreelancerID],
        [AgreementType],
        ROW_NUMBER() OVER (PARTITION BY [FreelancerID] ORDER BY [ActiveEndTime] DESC) AS RN
    FROM [EnterpriseDW].[Core].[Dim_WorkerProfile]
    WHERE [AgreementType] != 'Undefined'
),

-- 2. PORTAL ONBOARDING FLAGS
ExperienceCheck AS (
    SELECT DISTINCT FreelancerID, 1 AS HasVerifiedExperience
    FROM [Staging].[FreelancerApp].[Log_Experience] 
    WHERE FreelancerID IS NOT NULL
),

ComplianceDocCheck AS (
    SELECT DISTINCT FreelancerID, 1 AS HasComplianceDocs
    FROM [Staging].[FreelancerApp].[Log_Documents] 
    WHERE FreelancerID IS NOT NULL
),

TermsAcceptance AS (
    SELECT FreelancerID, 1 AS IsTOSAccepted, MAX(CreatedDate) AS AcceptanceDate
    FROM [Staging].[FreelancerApp].[Log_TermsOfService] 
    WHERE IsAccepted = 1 
    GROUP BY FreelancerID
),

AlertsCheck AS (
    SELECT FreelancerID, 1 AS HasActiveAlerts
    FROM [Staging].[FreelancerApp].[Log_Notifications] 
    WHERE IsDeleted = 0 
    GROUP BY FreelancerID
),

-- 3. ERP (IVALUA) SUPPLIER FLAGS
ERPSupplierCheck AS (
    SELECT 
        FreelancerID, 
        1 AS IsERPSynced, 
        MAX(CreatedDate) AS ERPCreationDate, 
        MAX(UpdatedDate) AS MSUpdateDate
    FROM [Staging].[FreelancerApp].[Log_ERPSupplier] 
    WHERE FreelancerID IS NOT NULL 
    GROUP BY FreelancerID
),

ERPRequestCheck AS (
    SELECT DISTINCT FreelancerID, 1 AS IsERPRequested
    FROM [Staging].[FreelancerApp].[Log_SupplierRequest] 
    WHERE FreelancerID IS NOT NULL
),

-- 4. FINANCIAL & ACTIVITY FLAGS
TimesheetCheck AS (
    SELECT DISTINCT FreelancerID, 1 AS IsActivityLogged
    FROM [Staging].[FreelancerApp].[Log_Timesheets] 
    WHERE FreelancerID IS NOT NULL
),

BillingCheck AS (
    SELECT DISTINCT FreelancerID, 1 AS IsInvoiced
    FROM [Staging].[FreelancerApp].[Log_Invoices]
    WHERE FreelancerID IS NOT NULL
),

CorporateInfoCheck AS (
    SELECT DISTINCT FreelancerID, 1 AS HasCorporateEntity
    FROM [Staging].[FreelancerApp].[Log_CorporateInfo]
    WHERE FreelancerID IS NOT NULL
)

-- ============================================================================
-- MAIN QUERY: Consolidate Master Table
-- ============================================================================
SELECT 
       -- === A. MASTER WORKER DATA ===
       wrk.[SystemKey]
      ,wrk.[FreelancerID]
      ,wrk.[InternalProfileID]
      ,wrk.[PrimaryEmail]
      ,wrk.[FirstName]
      ,wrk.[LastName]
      ,wrk.[ApproverID]
      ,appr.[Email] AS ApproverEmail
      ,appr.[LastName] + ' ' + appr.[FirstName] AS ApproverName
      ,wrk.[ProfileCreatedDate]
      ,wrk.[ActiveStartTime]
      ,wrk.[ActiveEndTime]
      ,wrk.[IsActive]
      ,wrk.[BusinessUnit]
      ,wrk.[WorkerType]
      ,wrk.[EngagementStatus]
      ,wrk.[AgreementType]
      
       -- === B. GEOGRAPHIC & DEMOGRAPHIC INFO ===
      ,geo.[CountryCode]
      ,geo.[CountryName]
      ,geo.[Region]
      ,cand.FirstName AS CandidateFirstName
      ,cand.LastName AS CandidateLastName
      ,cand.PersonalEmail

       -- === C. RECONCILIATION METRICS (Booleans & Dates) ===
      ,COALESCE(corp.HasCorporateEntity, 0)   AS IsCorporateEntityRegistered
      ,COALESCE(ts.IsActivityLogged, 0)       AS IsActivityLogged
      ,COALESCE(inv.IsInvoiced, 0)            AS IsInvoiced
      ,COALESCE(erp.IsERPSynced, 0)           AS IsERPSynced
      ,erp.ERPCreationDate                    AS ERPLastCreatedDate
      ,erp.MSUpdateDate                       AS ERPUpdatedDate
      ,COALESCE(req.IsERPRequested, 0)        AS IsERPRequested
      ,COALESCE(exp.HasVerifiedExperience, 0) AS IsExperienceVerified
      ,COALESCE(doc.HasComplianceDocs, 0)     AS AreComplianceDocsSubmitted
      ,COALESCE(tos.IsTOSAccepted, 0)         AS IsTOSAccepted
      ,tos.AcceptanceDate                     AS TOSAcceptanceDate
      ,COALESCE(alrt.HasActiveAlerts, 0)      AS HasActiveAlerts

  -- 1. Source Table: Worker Master Data
  FROM [EnterpriseDW].[Core].[Dim_WorkerProfile] wrk
  LEFT JOIN LatestAgreement la 
       ON wrk.FreelancerID = la.FreelancerID AND la.RN = 1
       
  -- 2. Geography and Organizational Joins
  LEFT JOIN [EnterpriseDW].[Core].[Dim_BusinessUnit] geo 
       ON wrk.BusinessUnit = geo.BusinessUnitCode 
  LEFT JOIN [Staging].[HR].[dbo_Worker] wrk_raw 
       ON wrk.FreelancerID = wrk_raw.FreelancerID
  LEFT JOIN [Staging].[HR].[dbo_Applicant] cand 
       ON wrk_raw.InternalProfileID = cand.InternalProfileID
  LEFT JOIN [Staging].[HR].[dbo_Worker] appr 
       ON wrk_raw.ApproverID = appr.FreelancerID
       
  -- 3. Flag CTE Joins
  LEFT JOIN CorporateInfoCheck corp ON corp.FreelancerID = wrk.FreelancerID
  LEFT JOIN ExperienceCheck exp     ON exp.FreelancerID = wrk.FreelancerID
  LEFT JOIN ComplianceDocCheck doc  ON doc.FreelancerID = wrk.FreelancerID
  LEFT JOIN TermsAcceptance tos     ON tos.FreelancerID = wrk.FreelancerID    
  LEFT JOIN AlertsCheck alrt        ON alrt.FreelancerID = wrk.FreelancerID
  LEFT JOIN ERPSupplierCheck erp    ON erp.FreelancerID = wrk.FreelancerID
  LEFT JOIN ERPRequestCheck req     ON req.FreelancerID = wrk.FreelancerID
  LEFT JOIN TimesheetCheck ts       ON ts.FreelancerID = wrk.FreelancerID
  LEFT JOIN BillingCheck inv        ON inv.FreelancerID = wrk.FreelancerID

WHERE 
    wrk.EmployedBy != 'INTERNAL_SELF_EMP'                  
    AND wrk.IsActive = 1                                     
    AND wrk.WorkerType = 'Consultant'                    
    AND wrk.EngagementStatus IN ('Active', 'Offboarding', 'Recruited') 
    AND geo.IsActiveEntity = 1                               
    
    -- Scope restricted to authorized regional hubs
    AND wrk.BusinessUnit IN (
        'EU_CZ', 'EU_PL', 'EU_TR', 'EU_HR', 'EU_FR', 'EU_CH', 'EU_GR', 'EU_UK',
        'LATAM_BR', 'LATAM_BR_CUR', 'LATAM_BR_RIO',
        'APAC_CN', 'APAC_WHN', 'APAC_SGP', 'APAC_MY', 'APAC_VN', 'APAC_JPN', 'APAC_THAI'
    )
    
    -- Ensure correct agreement type fallback
    AND (
        wrk.AgreementType = 'Freelance' 
        OR (wrk.AgreementType = 'Undefined' AND la.AgreementType = 'Freelance')
    )
ORDER BY 
    wrk.[FreelancerID] ASC;
