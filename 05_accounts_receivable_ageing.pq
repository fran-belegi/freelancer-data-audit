// ============================================================================
// Accounts receivable balance ageing (Power Query M on Synapse)
// ----------------------------------------------------------------------------
// Computes a running-balance ageing view for client invoices from the
// accounting balance sheet.
//
// Data flow:
//   1. `balsheet` CTE — pulls invoice-related entries from the balance sheet
//      (specific general ledger codes for client receivables), joined with
//      the GIT (General Item Table) for factor identification.
//   2. `recovery_histo` CTE — enriches each GIT entry with invoice metadata
//      and flags "internal" entries (transfers between GL accounts of equal
//      opposite amounts on the same day — these cancel out).
//   3. `recovery_histo_3` CTE — computes a running balance per invoice and
//      the next-entry date (or today if the invoice is not fully settled).
//   4. `overdue` CTE — keeps only invoices that were ever overdue.
//
// The M steps expand each entry into per-Friday reporting dates between
// consecutive GIT entries, compute the balance ageing bucket, and convert
// amounts to EUR via a currency dimension merge.
// ============================================================================
let
    Source = Sql.Database(
        "synapse-server.sql.azuresynapse.net",
        "processed",
        [Query =
            "with balsheet as (#(lf)" &
            "    select#(lf)" &
            "        b.[InvoiceText] as inv_text,#(lf)" &
            "        b.[LB_InvoiceId] as inv_business_id,#(lf)" &
            "        b.[ClientCode] as client_code,#(lf)" &
            "        b.[GeneralLedgerNumber] as general_ledger_number,#(lf)" &
            "        b.[InvoiceDate] as inv_created_at,#(lf)" &
            "        b.[InvoiceDueDate] as inv_billing_due_date,#(lf)" &
            "        b.[PaymentDate] as inv_payment_date,#(lf)" &
            "        b.[OccyInvoiceAmount_] as inv_amount,#(lf)" &
            "        b.[InvoicePayment] as inv_payment_amount,#(lf)" &
            "        (case when b.OccyInvoiceAmount_ is null then 0 else b.OccyInvoiceAmount_ end) +#(lf)" &
            "         (case when b.InvoicePayment is null then 0 else b.InvoicePayment end) as inv_balance,#(lf)" &
            "        case when gb.[GeneralLedgerNumber] = 411300 and gb.[ClientCode] like 'F%' then 1 else 0 end as inv_under_factor,#(lf)" &
            "        b.[ClientCurrencyId] as currency,#(lf)" &
            "        c.[CompanyId] as company_id#(lf)" &
            "    from [warehouse].[Fact_BalSheet] as b#(lf)" &
            "    left join [warehouse].[Dim_Company] as c on b.Company = c.Company#(lf)" &
            "    left join [warehouse].[Dim_GITBalSheet] as gb on b.InvoiceText = gb.TextLine1 and gb.GeneralLedgerNumber = 411300#(lf)" &
            "    where (b.[InvoiceText] like 'PLT-20%' or b.[InvoiceText] like 'INV-20%' or b.[InvoiceText] like 'RFA%')#(lf)" &
            "      and b.[FiscalYear] >= 2022#(lf)" &
            "      and b.[GeneralLedgerNumber] in (411151,411001,411152,411000,411300,411002,411150)#(lf)" &
            "      and b.[InvoiceText] is not null#(lf)" &
            "      and (gb.[invoiceNumber] <> 'UNALLOCATED' or gb.[invoiceNumber] is null)#(lf)" &
            "),#(lf)" &
            "recovery_histo as (#(lf)" &
            "    select distinct#(lf)" &
            "        b.inv_text, b.inv_business_id,#(lf)" &
            "        gb.ClientCode as client_code,#(lf)" &
            "        gb.GeneralLedgerNumber as general_ledger_number,#(lf)" &
            "        b.inv_created_at, b.inv_billing_due_date,#(lf)" &
            "        gb.EntryDate as git_entry_date,#(lf)" &
            "        gb.EntryId as git_entry_id,#(lf)" &
            "        isnull(b.inv_amount, 0) as inv_amount,#(lf)" &
            "        gb.OccyAmount as entry_amount,#(lf)" &
            "        b.currency#(lf)" &
            "    from balsheet as b#(lf)" &
            "    left join [warehouse].[Dim_GITBalSheet] as gb on b.inv_text = gb.InvoiceText and gb.invoiceNumber <> 'UNALLOCATED'#(lf)" &
            "),#(lf)" &
            "-- Flag internal entries: an entry that has a mirror entry with the exact opposite amount#(lf)" &
            "-- on the same day in a different GL account is a bookkeeping transfer (net zero) — exclude it.#(lf)" &
            "recovery_histo_2 as (#(lf)" &
            "    select rh1.*,#(lf)" &
            "        case when rh2.inv_text is not null then 1 else 0 end as is_internal#(lf)" &
            "    from recovery_histo as rh1#(lf)" &
            "    left join recovery_histo as rh2#(lf)" &
            "        on rh1.inv_text = rh2.inv_text#(lf)" &
            "       and rh1.entry_amount = -rh2.entry_amount#(lf)" &
            "       and rh1.general_ledger_number <> rh2.general_ledger_number#(lf)" &
            "       and rh1.git_entry_date = rh2.git_entry_date#(lf)" &
            "),#(lf)" &
            "-- Running balance per invoice + next-entry date (or today if invoice still open at final entry)#(lf)" &
            "recovery_histo_3 as (#(lf)" &
            "    select#(lf)" &
            "        rh.inv_text, rh.client_code, rh.general_ledger_number,#(lf)" &
            "        rh.inv_created_at, rh.inv_billing_due_date,#(lf)" &
            "        rh.inv_amount, rh.entry_amount,#(lf)" &
            "        sum(rh.entry_amount) over (#(lf)" &
            "            partition by rh.inv_text, rh.inv_billing_due_date#(lf)" &
            "            order by rh.git_entry_date, rh.git_entry_id#(lf)" &
            "            rows between unbounded preceding and current row#(lf)" &
            "        ) as balance_amount,#(lf)" &
            "        rh.currency,#(lf)" &
            "        rh.git_entry_date,#(lf)" &
            "        rh.git_entry_id,#(lf)" &
            "        case#(lf)" &
            "            when lead(rh.git_entry_date) over (partition by rh.inv_text order by rh.git_entry_date, rh.git_entry_id) is not null#(lf)" &
            "                then lead(rh.git_entry_date) over (partition by rh.inv_text order by rh.git_entry_date, rh.git_entry_id)#(lf)" &
            "            when lead(rh.git_entry_date) over (partition by rh.inv_text order by rh.git_entry_date, rh.git_entry_id) is null#(lf)" &
            "             and sum(rh.entry_amount) over (partition by rh.inv_text order by rh.git_entry_date, rh.git_entry_id#(lf)" &
            "                                            rows between unbounded preceding and current row) <> 0#(lf)" &
            "                then cast(getdate() as date)#(lf)" &
            "            else rh.git_entry_date#(lf)" &
            "        end as next_entry_date#(lf)" &
            "    from recovery_histo_2 as rh#(lf)" &
            "    where rh.is_internal = 0#(lf)" &
            "),#(lf)" &
            "min_max_entry_date as (#(lf)" &
            "    select inv_text, max(next_entry_date) as max_entry_date#(lf)" &
            "    from recovery_histo_3 group by inv_text, inv_billing_due_date#(lf)" &
            "),#(lf)" &
            "-- Keep only invoices that were overdue at least once#(lf)" &
            "overdue as (#(lf)" &
            "    select inv_text,#(lf)" &
            "        max(case when balance_amount = 0 and cast(git_entry_date as date) <= cast(inv_billing_due_date as date) then 0 else 1 end) as overdue#(lf)" &
            "    from recovery_histo_3 group by inv_text#(lf)" &
            ")#(lf)" &
            "select rh.*, med.max_entry_date#(lf)" &
            "from recovery_histo_3 as rh#(lf)" &
            "left join min_max_entry_date as med on rh.inv_text = med.inv_text#(lf)" &
            "left join overdue as o on rh.inv_text = o.inv_text#(lf)" &
            "where o.overdue = 1#(lf)" &
            "  and rh.client_code not like 'AC%'#(lf)" &
            "order by rh.inv_text, rh.git_entry_date, rh.git_entry_id asc"
        ]
    ),

    // Enrich with invoice attributes from the union of AR/INV facts
    #"Merged Queries" = Table.NestedJoin(Source, {"inv_text"}, invoices_union, {"inv_business_id"}, "InvoiceLookup", JoinKind.LeftOuter),
    #"Expanded Invoice Balance" = Table.ExpandTableColumn(#"Merged Queries", "InvoiceLookup",
        {"inv_type", "po_sk", "service_month", "is_client", "final_client_id", "legal_entity_id", "inv_validated_at", "inv_billing_due_date", "project_id", "po_category"},
        {"inv_type", "po_sk", "service_month", "is_client", "final_client_id", "legal_entity_id", "inv_validated_at", "inv_billing_due_date.1", "project_id", "po_category"}
    ),
    #"Filtered Rows" = Table.SelectRows(#"Expanded Invoice Balance", each ([is_client] = true) and [service_month] >= #date(2022, 1, 1)),

    #"Changed Type" = Table.TransformColumnTypes(#"Filtered Rows", {
        {"inv_created_at", type date}, {"inv_billing_due_date", type date},
        {"git_entry_date", type date}, {"next_entry_date", type date},
        {"max_entry_date", type date}, {"inv_billing_due_date.1", type date},
        {"inv_validated_at", type date}
    }),

    // Prefer the invoice fact's due date when available; else keep the accounting one
    #"Added Conditional Column1" = Table.AddColumn(#"Changed Type", "inv_billing_due_date_2",
        each if [inv_billing_due_date.1] <> null then [inv_billing_due_date.1] else [inv_billing_due_date]
    ),
    #"Removed Columns" = Table.RemoveColumns(#"Added Conditional Column1", {"is_client", "inv_billing_due_date", "inv_billing_due_date.1"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Columns", {{"inv_billing_due_date_2", "inv_billing_due_date"}}),
    #"Reordered Columns" = Table.ReorderColumns(#"Renamed Columns", {"project_id", "po_sk", "inv_text", "client_code", "general_ledger_number", "service_month", "inv_created_at", "inv_billing_due_date", "git_entry_date", "git_entry_id", "next_entry_date", "inv_amount", "entry_amount", "balance_amount", "currency"}),
    #"Filtered Rows1" = Table.SelectRows(#"Reordered Columns", each true),
    #"Reordered Columns1" = Table.ReorderColumns(#"Filtered Rows1", {"project_id", "po_sk", "inv_text", "client_code", "general_ledger_number", "currency", "inv_amount", "entry_amount", "balance_amount", "inv_created_at", "inv_validated_at", "inv_billing_due_date", "git_entry_date", "max_entry_date", "git_entry_id", "next_entry_date", "service_month"}),

    // Expand each entry into per-reporting-date rows between consecutive GIT entries
    #"Add Date List" = Table.AddColumn(#"Reordered Columns1", "date_list", each
        let
            // Start of the reporting window: whichever is later — GIT entry or invoice validation/creation
            validatedOrCreated = if [inv_validated_at] <> null then [inv_validated_at] else [inv_created_at],
            startDate = if [git_entry_date] < validatedOrCreated then validatedOrCreated else [git_entry_date],
            endDate = [next_entry_date],
            isFirstLine = [git_entry_date] < validatedOrCreated,
            isLastLine = [git_entry_date] = [max_entry_date],

            durationDays = Duration.Days(endDate - startDate),
            fullRange = if durationDays >= 0
                        then List.Dates(startDate, durationDays + 1, #duration(1,0,0,0))
                        else {},

            // 1. All Fridays in the window
            fridayList = List.Select(fullRange, each Date.DayOfWeek(_, Day.Monday) = 4),
            // 2. Today
            currentDate = Date.From(DateTime.LocalNow()),
            withCurrent = if currentDate >= startDate and currentDate <= endDate
                          then List.Combine({fridayList, {currentDate}})
                          else fridayList,
            // 3. Tomorrow
            tomorrowDate = Date.AddDays(currentDate, 1),
            withTomorrow = if tomorrowDate >= startDate and tomorrowDate <= endDate
                           then List.Combine({withCurrent, {tomorrowDate}})
                           else withCurrent,
            // 4. First line: also add the invoice due date
            withStart = if isFirstLine and not List.Contains(withTomorrow, [inv_billing_due_date])
                        then List.Combine({{[inv_billing_due_date]}, withTomorrow})
                        else withTomorrow,
            // 5. Last line: also add end date
            withEnd = if isLastLine and not List.Contains(withStart, endDate)
                      then List.Combine({withStart, {endDate}})
                      else withStart,

            finalList = List.Sort(List.Distinct(withEnd), Order.Ascending)
        in
            finalList
    ),

    #"Expanded Date List" = Table.ExpandListColumn(#"Add Date List", "date_list"),

    // For each (po_sk, invoice, date), keep the most recent GIT entry (largest git_entry_date)
    Custom1 = Table.Group(
        #"Expanded Date List",
        {"po_sk", "inv_text", "date_list"},
        {{"AllRows", each Table.FirstN(Table.Sort(_, {{"git_entry_date", Order.Descending}}), 1), type table}}
    ),
    #"Expanded AllRows" = Table.ExpandTableColumn(Custom1, "AllRows",
        {"project_id", "po_sk", "inv_text", "client_code", "general_ledger_number", "currency", "inv_amount", "entry_amount", "balance_amount", "inv_created_at", "inv_billing_due_date", "git_entry_date", "max_entry_date", "git_entry_id", "next_entry_date", "service_month", "date_list", "inv_validated_at", "inv_type", "po_category", "final_client_id", "legal_entity_id"},
        {"project_id", "po_sk.1", "inv_text.1", "client_code", "general_ledger_number", "currency", "inv_amount", "entry_amount", "balance_amount", "inv_created_at", "inv_billing_due_date", "git_entry_date", "max_entry_date", "git_entry_id", "next_entry_date", "service_month", "date_list.1", "inv_validated_at", "inv_type", "po_category", "final_client_id", "legal_entity_id"}
    ),
    #"Removed Columns1" = Table.RemoveColumns(#"Expanded AllRows", {"max_entry_date", "git_entry_id", "date_list.1", "po_sk.1", "inv_text.1"}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Removed Columns1", {
        {"date_list", type date}, {"general_ledger_number", Int64.Type}, {"currency", type text},
        {"inv_amount", type number}, {"entry_amount", type number}, {"balance_amount", type number},
        {"inv_created_at", type date}, {"inv_billing_due_date", type date},
        {"git_entry_date", type date}, {"next_entry_date", type date},
        {"service_month", type date}, {"inv_validated_at", type date}
    }),
    #"Reordered Columns2" = Table.ReorderColumns(#"Changed Type1", {"po_sk", "inv_text", "client_code", "project_id", "general_ledger_number", "currency", "inv_amount", "entry_amount", "balance_amount", "inv_created_at", "inv_validated_at", "inv_billing_due_date", "git_entry_date", "next_entry_date", "service_month", "date_list"}),
    #"Filtered Rows2" = Table.SelectRows(#"Reordered Columns2", each ([date_list] <> null and [date_list] >= #date(2023, 1, 1))),

    // Friday snapshot flag
    #"Add is_friday" = Table.AddColumn(#"Filtered Rows2", "is_friday",
        each if Date.DayOfWeek([date_list], Day.Monday) = 4 or [date_list] = Date.From(DateTime.LocalNow())
             then 1 else 0,
        Int64.Type
    ),

    // Ageing buckets from the invoice due date
    #"Add balance_ageing_id" = Table.AddColumn(#"Add is_friday", "balance_ageing_id",
        each
            let
                diffJours = Duration.Days([date_list] - [inv_billing_due_date])
            in
                if [date_list] < [inv_billing_due_date] then 0
                else if diffJours = 0             then 0   // M
                else if diffJours <= 30           then 1   // M-1
                else if diffJours <= 90           then 2   // M-2 to M-3
                else if diffJours <= 180          then 3   // M-4 to M-6
                else if diffJours <= 270          then 4   // M-7 to M-9
                else if diffJours <= 360          then 5   // M-10 to M-12
                else                                   6,  // > M-12
        Int64.Type
    ),

    // EUR conversion
    #"Added Conditional Column" = Table.AddColumn(#"Add balance_ageing_id", "currency_date",
        each if [service_month] < DateTime.Date(DateTime.LocalNow()) then [service_month] else Date.AddDays(DateTime.Date(DateTime.LocalNow()), -1)
    ),
    #"Merged Dim Currency" = Table.NestedJoin(#"Added Conditional Column", {"currency", "currency_date"}, #"currency dim", {"currency", "date"}, "Dim Currency", JoinKind.LeftOuter),
    #"Expanded Dim Currency" = Table.ExpandTableColumn(#"Merged Dim Currency", "Dim Currency", {"daily_rate", "monthly_rate"}, {"daily_rate", "monthly_rate"}),
    #"Add balance_amount_eur" = Table.AddColumn(#"Expanded Dim Currency", "balance_amount_eur",
        each Number.Round([balance_amount] / [monthly_rate], 2)
    ),
    #"Changed Type2" = Table.TransformColumnTypes(#"Add balance_amount_eur", {{"balance_amount_eur", type number}})
in
    #"Changed Type2"
