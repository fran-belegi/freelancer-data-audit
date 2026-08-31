-- ============================================================================
-- Invoice amounts transformation
-- ----------------------------------------------------------------------------
-- Builds an enriched invoice dimension from the raw invoice fact table.
-- Handles multiple invoice types (Invoice C/P, Credit Notes, Complementary),
-- computes net and gross amounts (including nested product_service arrays),
-- OCD (on-call duty), expenses, regulations and withholding tax.
--
-- Sign convention: Client-side invoices (C, Complementary C, Credit Note M)
-- carry positive amounts; supplier-side invoices (P, Credit Note N) carry
-- negative amounts. Package work_unit_type invoices without inv_id are NULL.
-- ============================================================================

WITH base_invoice_data AS (
    SELECT
      i.* EXCEPT(
        refused, mantu_status, upload_status, lbc_core_data, inv_core_data,
        expense, ocd, product_service,
        withholding_tax, regulation, amount, finally_payment_workflow, finally_comment
      ),
      p.po_validation_date,
      inv_core_data.country_code AS billing_country,
      amount.work_unit_type AS work_unit_type,
      amount.payment_term.payment_term_name AS payment_term_name,
      amount.tax,

      -- Invoice generation type (source: company configuration flag)
      CASE
        WHEN c1.generateinvoicetype = 0 THEN 'Electronic Self-Billing'
        WHEN c1.generateinvoicetype = 1 THEN 'Manual Upload'
        WHEN c1.generateinvoicetype = 3 THEN 'Electronic invoice issuance'
        ELSE NULL
      END AS invoice_generation_type,

      element_at(i.finally_payment_workflow, -1).to_status AS finally_status_raw,
      element_at(
        filter(
          i.finally_payment_workflow,
          x -> x.to_status IN (
            'PAID', 'PAID_MANUALLY', 'PAYMENT_ORDER_SENT',
            'PAYMENT_VALIDATED', 'READY_FOR_PAYMENT'
          )
        ),
        1
      ).triggered_at AS finally_validation_date,

      -- ------------------------------------------------------------------
      -- Net amount computation
      -- Complementary invoices carry line items in product_service (array
      -- of struct); other types use flat amount.price * amount.quantity.
      -- ------------------------------------------------------------------
      CASE
        WHEN inv_type = 'Invoice Complementary C' THEN
          COALESCE(
            AGGREGATE(
              ZIP_WITH(
                product_service.price,
                product_service.quantity,
                (p2, q) -> COALESCE(p2, 0) * COALESCE(q, 0)
              ),
              CAST(0.0 AS DOUBLE),
              (acc, x) -> acc + x
            ),
            0
          )
        ELSE COALESCE(amount.price * amount.quantity, 0)
      END AS raw_inv_amount_net,

      -- Gross amount (with VAT). Tax is stored as an array of struct with
      -- tax_percentage as string; we cast and sum defensively.
      CASE
        WHEN inv_type = 'Invoice Complementary C' THEN
          ROUND(
            COALESCE(
              AGGREGATE(
                ZIP_WITH(
                  ZIP_WITH(
                    product_service.price,
                    product_service.quantity,
                    (p2, q) -> COALESCE(p2, 0) * COALESCE(q, 0)
                  ),
                  product_service.tax_percentage,
                  (subtotal, tax_array) -> subtotal * (
                    1 + COALESCE(
                      AGGREGATE(
                        tax_array,
                        CAST(0.0 AS DOUBLE),
                        (acc, t) -> acc + COALESCE(TRY_CAST(t AS DOUBLE), 0.0)
                      ),
                      0.0
                    ) / 100
                  )
                ),
                CAST(0.0 AS DOUBLE),
                (acc, x) -> acc + x
              ),
              0
            ),
            2
          )
        ELSE
          ROUND(
            COALESCE(amount.price * amount.quantity, 0) * (
              1 + COALESCE(
                AGGREGATE(
                  amount.tax,
                  CAST(0.0 AS DOUBLE),
                  (acc, t) -> acc + COALESCE(TRY_CAST(t.tax_percentage AS DOUBLE), 0.0)
                ),
                0.0
              ) / 100
            ),
            2
          )
      END AS raw_inv_amount_gross,

      -- On-call duty (OCD) net/gross
      COALESCE(ocd.price * ocd.quantity, 0) AS raw_inv_ocd_net,
      ROUND(
        COALESCE(ocd.price * ocd.quantity, 0) * (
          1 + COALESCE(
            AGGREGATE(
              ocd.tax_percentage,
              CAST(0.0 AS DOUBLE),
              (acc, t) -> acc + COALESCE(TRY_CAST(t AS DOUBLE), 0.0)
            ),
            0.0
          ) / 100
        ),
        2
      ) AS raw_inv_ocd_gross,

      -- Expenses net/gross (array of line items with per-line VAT)
      COALESCE(
        ROUND(
          AGGREGATE(
            expense,
            CAST(0.0 AS DOUBLE),
            (acc, x) -> acc + COALESCE(x.quantity * x.price, 0.0)
          ),
          2
        ),
        0
      ) AS raw_inv_expense_without_vat,

      COALESCE(
        ROUND(
          AGGREGATE(
            expense,
            CAST(0.0 AS DOUBLE),
            (acc, x) ->
              acc + (
                COALESCE(x.quantity * x.price, 0.0) * (
                  1 + COALESCE(
                    AGGREGATE(
                      x.tax,
                      CAST(0.0 AS DOUBLE),
                      (acc_tax, t) -> acc_tax + COALESCE(TRY_CAST(t.tax_percentage AS DOUBLE), 0.0)
                    ),
                    0.0
                  ) / 100
                )
              )
          ),
          2
        ),
        0
      ) AS raw_inv_expense_with_vat,

      -- Regulation net/gross
      COALESCE(regulation.price * regulation.quantity, 0) AS raw_inv_reg_net,
      ROUND(
        COALESCE(regulation.price * regulation.quantity, 0) * (
          1 + COALESCE(
            AGGREGATE(
              regulation.tax_percentage,
              CAST(0.0 AS DOUBLE),
              (acc, t) -> acc + COALESCE(TRY_CAST(t AS DOUBLE), 0.0)
            ),
            0.0
          ) / 100
        ),
        2
      ) AS raw_inv_reg_gross,

      -- Withholding tax
      COALESCE(
        AGGREGATE(
          withholding_tax.withholding_tax_percentage,
          CAST(0.0 AS DOUBLE),
          (acc, w) -> acc + COALESCE(TRY_CAST(w AS DOUBLE), 0.0)
        ),
        0.0
      ) AS withholding_percentage,

      ROUND(
        COALESCE(amount.price * amount.quantity, 0) * (
          COALESCE(
            AGGREGATE(
              withholding_tax.withholding_tax_percentage,
              CAST(0.0 AS DOUBLE),
              (acc, w) -> acc + COALESCE(TRY_CAST(w AS DOUBLE), 0.0)
            ),
            0.0
          ) / 100
        ),
        2
      ) AS withholding_amount

    FROM warehouse.dim_invoice AS i
    LEFT JOIN (
        SELECT po_sk, po_validation_date, supplier_id
        FROM warehouse.dim_project
        WHERE is_valid = true
    ) AS p ON i.po_sk = p.po_sk
    LEFT JOIN warehouse.dim_supplier_company c ON p.supplier_id = c.platform_id
    LEFT JOIN raw.companies c1 ON c1.id = c.platform_id
),

-- ---------------------------------------------------------------------------
-- Apply sign convention based on invoice type. Client-side invoices are
-- positive, supplier-side are negative. Package types without inv_id → NULL.
-- ---------------------------------------------------------------------------
base AS (
    SELECT
      *,
      CASE
        WHEN inv_id IS NULL AND work_unit_type = 'Package' THEN NULL
        WHEN inv_type IN ('Invoice C', 'Credit Note M', 'Invoice Complementary C') THEN ABS(raw_inv_amount_net)
        WHEN inv_type IN ('Invoice P', 'Credit Note N') OR (is_client = false AND inv_type IS NULL) THEN -ABS(raw_inv_amount_net)
        ELSE raw_inv_amount_net
      END AS inv_amount_without_vat,

      CASE
        WHEN inv_id IS NULL AND work_unit_type = 'Package' THEN NULL
        WHEN inv_type IN ('Invoice C', 'Credit Note M', 'Invoice Complementary C') THEN ABS(raw_inv_amount_gross)
        WHEN inv_type IN ('Invoice P', 'Credit Note N') OR (is_client = false AND inv_type IS NULL) THEN -ABS(raw_inv_amount_gross)
        ELSE raw_inv_amount_gross
      END AS inv_amount_with_vat,

      CASE
        WHEN inv_id IS NULL AND work_unit_type = 'Package' THEN NULL
        WHEN inv_type IN ('Invoice C', 'Credit Note M') THEN ABS(raw_inv_ocd_net)
        WHEN inv_type IN ('Invoice P', 'Credit Note N') OR (is_client = false AND inv_type IS NULL) THEN -ABS(raw_inv_ocd_net)
        ELSE raw_inv_ocd_net
      END AS inv_ocd_without_vat,

      CASE
        WHEN inv_id IS NULL AND work_unit_type = 'Package' THEN NULL
        WHEN inv_type IN ('Invoice C', 'Credit Note M') THEN ABS(raw_inv_ocd_gross)
        WHEN inv_type IN ('Invoice P', 'Credit Note N') OR (is_client = false AND inv_type IS NULL) THEN -ABS(raw_inv_ocd_gross)
        ELSE raw_inv_ocd_gross
      END AS inv_ocd_with_vat,

      CASE
        WHEN inv_id IS NULL AND work_unit_type = 'Package' THEN NULL
        WHEN inv_type IN ('Invoice C', 'Credit Note M') THEN ABS(raw_inv_expense_without_vat)
        WHEN inv_type IN ('Invoice P', 'Credit Note N') OR (is_client = false AND inv_type IS NULL) THEN -ABS(raw_inv_expense_without_vat)
        ELSE raw_inv_expense_without_vat
      END AS inv_expense_without_vat,

      CASE
        WHEN inv_id IS NULL AND work_unit_type = 'Package' THEN NULL
        WHEN inv_type IN ('Invoice C', 'Credit Note M') THEN ABS(raw_inv_expense_with_vat)
        WHEN inv_type IN ('Invoice P', 'Credit Note N') OR (is_client = false AND inv_type IS NULL) THEN -ABS(raw_inv_expense_with_vat)
        ELSE raw_inv_expense_with_vat
      END AS inv_expense_with_vat,

      CASE
        WHEN inv_id IS NULL AND work_unit_type = 'Package' THEN NULL
        WHEN inv_type IN ('Invoice C', 'Credit Note M') THEN ABS(raw_inv_reg_net)
        WHEN inv_type IN ('Invoice P', 'Credit Note N') OR (is_client = false AND inv_type IS NULL) THEN -ABS(raw_inv_reg_net)
        ELSE raw_inv_reg_net
      END AS inv_regulation_without_vat,

      CASE
        WHEN inv_id IS NULL AND work_unit_type = 'Package' THEN NULL
        WHEN inv_type IN ('Invoice C', 'Credit Note M') THEN ABS(raw_inv_reg_gross)
        WHEN inv_type IN ('Invoice P', 'Credit Note N') OR (is_client = false AND inv_type IS NULL) THEN -ABS(raw_inv_reg_gross)
        ELSE raw_inv_reg_gross
      END AS inv_regulation_with_vat

    FROM base_invoice_data
)

SELECT
  * EXCEPT(
    raw_inv_amount_net,
    raw_inv_amount_gross,
    raw_inv_ocd_net,
    raw_inv_ocd_gross,
    raw_inv_expense_without_vat,
    raw_inv_expense_with_vat,
    raw_inv_reg_net,
    raw_inv_reg_gross
  ),
  ROUND(
    COALESCE(inv_amount_without_vat, 0) +
    COALESCE(inv_ocd_without_vat, 0) +
    COALESCE(inv_regulation_without_vat, 0),
    2
  ) AS inv_amount_without_vat_total,
  ROUND(
    COALESCE(inv_amount_with_vat, 0) +
    COALESCE(inv_ocd_with_vat, 0) +
    COALESCE(inv_regulation_with_vat, 0),
    2
  ) AS inv_amount_with_vat_total
FROM base
