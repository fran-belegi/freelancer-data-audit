# freelancer-data-audit
SQL &amp; Power BI (DAX) framework for auditing and reconciling activity data between an internal Freelancer App and ERP (Ivalua) invoices.
## 🔒 Confidentiality & Data Privacy Notice
Please note that all code in this repository has been heavily anonymized to comply with strict Non-Disclosure Agreements (NDA) and corporate security policies. 
- **Table and Column Names:** Altered to generic industry standards.
- **Business Logic:** Geopolitical entities, internal IDs, and specific vendor codes have been masked or generalized.
- **Data:** No real records, CSVs, or databases are included.
The underlying SQL complexity, feature engineering logic, and data architecture patterns remain fully intact to demonstrate my technical capabilities and problem-solving approach.
## 🛠 Tech Stack
- **Databricks Spark SQL:** Core transformation logic for invoice amounts and legal documents compliance, plus the wrapped queries embedded in the Power BI M pipelines.
- **Azure Synapse SQL:** Accounting balance sheet ageing with running balances and internal-transfer detection.
- **Power Query M:** Row-level temporal expansion, working-day arithmetic and multi-currency conversion inside Power BI.
- **Power BI:** End deliverable. DAX measures and semantic model relationships are not included in this repository.
## 💡 Patterns Demonstrated
- **Nested array processing:** Aggregations and zip operations over structs-of-structs and arrays-of-arrays.
- **Window functions:** Deduplication, running balances (SUM OVER), and next-event lookup (LEAD).
- **Row generation:** LATERAL VIEW EXPLODE in Spark and per-row list expansion (List.Dates) in M.
- **Full-outer join synthesis:** Built via UNION ALL of LEFT JOIN and LEFT ANTI JOIN where the engine lacks native support.
- **Multi-source dimension unification:** Merging heterogeneous entities (RFPs, quotes, POs) into a single dimension with type discrimination.
- **Multi-currency conversion:** Consistent EUR reporting via a shared currency dimension merge.
- **Defensive data handling:** TRY_CAST for stringly-typed numerics and neutralisation of overflow dates.
- **Prorata and working-day arithmetic:** Partial-month unit consumption and business-day counting.
- **Hybrid raw + processed pipelines:** Clear architectural documentation explaining when and why to cross data-warehouse layers.
