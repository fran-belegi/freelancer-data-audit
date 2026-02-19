/* ============================================================================
   PROJECT: ERP & Freelancer Portal Reconciliation Framework
   SCRIPT: 03_erp_invoice_reconciliation.sql
   
   DESCRIPTION: 
   This is the core reconciliation engine. It bridges the gap between the 
   Internal Freelancer Portal (Front-end inputs) and the ERP System (Ivalua).
   It matches invoices generated in the portal with the historized financial 
   records in the ERP, resolving status codes into readable labels and auditing 
   the end-to-end timeline of every transaction.
   ============================================================================ */

WITH 
-- ============================================================================
-- 1. ERP STATUS DICTIONARY: Lookup table for invoice statuses
-- ============================================================================
ERP_Status_Dictionary AS (
    SELECT 
        [status] AS StatusCode,
        [label] AS StatusDescription
    FROM [Staging].[ERP].[StatusLookup]
    WHERE [tdesc_name] = 't_ord_invoice'
),

-- ============================================================================
-- 2. SUPPLIER MAPPING: Get the most recent ERP Supplier ID per Freelancer
-- ============================================================================
SupplierMapping AS (
    SELECT 
        [FreelancerID], 
        MAX([ERPSupplierID]) AS [SupplierID],
        MAX([CreatedDate]) AS [ERPCreatedDate], 
        MAX([UpdatedDate]) AS [ERPUpdatedDate]
    FROM [Staging].[FreelancerApp].[Log_ERPSupplier]
    GROUP BY [FreelancerID]
)

-- ============================================================================
-- MAIN QUERY: End-to-End Invoice Reconciliation
-- ============================================================================
SELECT 
    -- A. IDENTIFIERS (Portal vs ERP)
    portal.[ERPInvoiceNumber],
    portal.[PortalInvoiceID],
    portal.[ERPTechnicalInvoiceID],
    portal.[FreelancerID],
    
    -- B. SUPPLIER RECONCILIATION
    sup.[SupplierID] AS [Mapped_SupplierID],
    erp_hist.[supplier_id] AS [ERP_Actual_SupplierID],
    sup.[ERPCreatedDate] AS [Supplier_Sync_Date],        
    sup.[ERPUpdatedDate] AS [Supplier_Last_Update],      

    -- C. FINANCIAL & STATUS DATA (The Core Reconciliation)
    erp_hist.[invoice_code] AS [ERP_Invoice_Code],
    portal.[StatusId] AS [Portal_StatusID],
    erp_hist.[status_code] AS [ERP_StatusCode],
    dict.[StatusDescription] AS [ERP_Status_Label], -- <== HERE IS THE JOINED LABEL
    erp_hist.[RejectionReason],
    
    -- D. MONETARY VALUES
    erp_hist.[total_amount_excl_tax] AS [Amount_Net],
    erp_hist.[total_amount_incl_tax] AS [Amount_Gross],
    erp_hist.[currency_id] AS [Currency],

    -- E. TIMELINE AUDIT (Portal vs ERP Dates)
    portal.[InvoiceDate] AS [Portal_Invoice_Date],        
    portal.[CreatedDate] AS [Portal_Created_Date],        
    erp_hist.[received_date] AS [ERP_Received_Date], 
    erp_hist.[due_date] AS [ERP_Due_Date],           
    erp_hist.[payment_date] AS [ERP_Payment_Date],   
    
    -- F. SYSTEM VALIDITY FLAGS
    portal.[IsDraft],
    erp_hist.[Active] AS [Is_ERP_Record_Active]

FROM [Staging].[FreelancerApp].[Log_Invoice] AS portal

-- 1. Attach Supplier Mapping to link the freelancer to the ERP
LEFT JOIN SupplierMapping AS sup 
    ON portal.[FreelancerID] = sup.[FreelancerID]

-- 2. Union with the HISTORIZED ERP Invoices table (The Financial Truth)
LEFT JOIN [EnterpriseDW].[ERP].[Dim_HistorizedInvoices] AS erp_hist 
    ON portal.[ERPInvoiceNumber] = erp_hist.[supplier_invoice_number] 
    AND sup.[SupplierID] = erp_hist.[supplier_id]

-- 3. JOIN the Status Dictionary to translate the status code to readable text
LEFT JOIN ERP_Status_Dictionary AS dict
    ON erp_hist.[status_code] = dict.[StatusCode]

-- Filter to only keep active historized records (prevents record inflation)
WHERE 
    (erp_hist.[Active] = 1 OR erp_hist.[invoice_id] IS NULL)

ORDER BY 
    portal.[PortalInvoiceID] ASC;
