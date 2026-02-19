/* ============================================================================
   PROJECT: ERP & Freelancer Portal Reconciliation Framework
   SCRIPT: 02_profile_feature_engineering.sql
   
   DESCRIPTION: 
   This script performs advanced Feature Engineering to enrich the Master Worker 
   Profile. It resolves 1-to-Many entity relationships (historical bank accounts, 
   multiple compliance documents, and highly normalized address structures) by 
   applying Window Functions to extract the single valid 'Source of Truth' 
   without altering the base data grain.
   
   KEY TRANSFORMATIONS:
   - Financial: Deduplicates and extracts the most recent 'Invoice' bank account.
   - Compliance: Pivots legal document existence into binary flags (Incorporation/Bank).
   - Geographic: Navigates a 6-tier normalized schema to retrieve the latest Billing Address.
   ============================================================================ */

WITH 
-- ============================================================================
-- 1. FINANCIAL FEATURES: Retrieve the most recent active bank account
-- ============================================================================
LatestBankInfo AS (
    SELECT 
        emp.[FreelancerID],
        bankInfo.[BankInformationId],
        bankType.[Label] AS AccountPurpose,
        bankInfo.[Label] AS BankName,
        bankInfo.[BIC],
        bankInfo.[IBAN],
        sys.[Label] AS BankingSystem,
        -- Deduplication: Assign 1 to the most recently added bank record
        ROW_NUMBER() OVER(
            PARTITION BY emp.[FreelancerID] 
            ORDER BY bankInfo.[BankInformationId] DESC 
        ) as Rn
    FROM [Staging].[Finance].[Raw_BankInformation_Link] bank
    JOIN [Staging].[HR].[Raw_InternalRecord] rec      ON bank.[InternalRecordID] = rec.[InternalRecordID]
    JOIN [Staging].[HR].[Raw_Worker] emp              ON rec.[FreelancerID] = emp.[FreelancerID]
    JOIN [Staging].[Finance].[Raw_BankDetails] bankInfo ON bank.[BankInformationId] = bankInfo.[BankInformationId]
    JOIN [Staging].[Finance].[Raw_BankAccountType] bankType ON bank.[BankInformationTypeId] = bankType.[BankInformationTypeId]
    JOIN [Staging].[Finance].[Raw_BankingSystem] sys  ON bankInfo.[BankInformationSystemId] = sys.[BankInformationSystemId]
    WHERE 
        (bankInfo.[IsDisabled] IS NULL OR bankInfo.[IsDisabled] = 0)
        AND bankType.[Label] = 'Invoices' 
),

-- ============================================================================
-- 2. COMPLIANCE FEATURES: Pivot essential legal documents into flags
-- ============================================================================
ComplianceDocs AS (
    SELECT 
        docInfo.[FreelancerID],
        MAX(CASE WHEN cat.[Label] = 'Incorporation' THEN 1 ELSE 0 END) AS HasIncorporationDoc,
        MAX(CASE WHEN cat.[CategoryId] = 20 THEN 1 ELSE 0 END) AS HasBankVerificationDoc
    FROM [Staging].[Compliance].[Raw_DocumentInfo] docInfo
    JOIN [Staging].[Compliance].[Raw_DocumentCategory] cat 
        ON docInfo.[CategoryId] = cat.[CategoryId]
    WHERE 
        docInfo.[IsDisabled] = 0
    GROUP BY docInfo.[FreelancerID]
),

-- ============================================================================
-- 3. GEOGRAPHIC FEATURES: Extract the latest valid Invoicing Address
-- ============================================================================
LatestBillingAddress AS (
    SELECT 
        emp.[FreelancerID] 
        ,addrType.[Label] AS AddressType
        ,addr.[StreetLine] AS AddressLine
        ,addr.[ZipCode]
        ,country.[CountryName] AS Country 
        ,city.[CityName] AS City
        -- Ranking: Assign 1 to the most recent address based on ID
        ,ROW_NUMBER() OVER(
            PARTITION BY emp.[FreelancerID] 
            ORDER BY addr.[AddressID] DESC 
        ) as Rn
    FROM [Staging].[HR].[Raw_Worker] emp
    JOIN [Staging].[HR].[Raw_InternalRecord] admin    ON emp.[FreelancerID] = admin.[FreelancerID]
    JOIN [Staging].[Geography].[Raw_RecordAddress] link ON admin.[InternalRecordID] = link.[InternalRecordID]
    JOIN [Staging].[Geography].[Raw_AddressMaster] addr ON link.[AddressID] = addr.[AddressID]
    JOIN [Staging].[Geography].[Raw_AddressType] addrType ON link.[AddressTypeID] = addrType.[AddressTypeID]
    JOIN [Staging].[Geography].[Raw_City] city        ON addr.[CityID] = city.[CityID]
    JOIN [Staging].[Geography].[Raw_Country] country  ON addr.[CountryID] = country.[CountryID]
    WHERE 
        addrType.[Label] = 'Invoicing'
)

-- ============================================================================
-- MAIN QUERY: Enrich the Master Profile with Engineered Features
-- ============================================================================
SELECT 
    -- A. Core Identifiers
    Master.[FreelancerID],
    
    -- B. Financial Features
    Bank.[BankName],
    Bank.[BIC],
    Bank.[IBAN],
    Bank.[BankingSystem],
    
    -- C. Geographic Features (Billing Address)
    Addr.[AddressLine],
    Addr.[ZipCode],
    Addr.[City],
    Addr.[Country],

    -- D. Compliance Flags
    COALESCE(Docs.[HasIncorporationDoc], 0) AS HasIncorporationDoc,
    COALESCE(Docs.[HasBankVerificationDoc], 0) AS HasBankVerificationDoc

FROM [EnterpriseDW].[Core].[Dim_WorkerProfile] Master

-- Join Financials (Rank 1 only)
LEFT JOIN LatestBankInfo Bank 
    ON Master.[FreelancerID] = Bank.[FreelancerID] 
    AND Bank.Rn = 1

-- Join Address (Rank 1 only)
LEFT JOIN LatestBillingAddress Addr 
    ON Master.[FreelancerID] = Addr.[FreelancerID] 
    AND Addr.Rn = 1

-- Join aggregated compliance documents
LEFT JOIN ComplianceDocs Docs 
    ON Master.[FreelancerID] = Docs.[FreelancerID]

WHERE 
-- same filters as previous script
ORDER BY 
    Master.[FreelancerID] ASC;
