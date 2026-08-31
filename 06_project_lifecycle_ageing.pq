// ============================================================================
// Project lifecycle ageing (Power Query M)
// ----------------------------------------------------------------------------
// Loads the project dimension with its full validation workflow (a nested
// array of status transitions), explodes it into per-status periods, and
// expands each period into a list of "reporting dates" (all Fridays plus
// today, tomorrow, entry/exit anchors) capped at 12 months from today.
//
// For each reporting date, computes:
//   - Pending status flags (Pending renewal, Pending PO, Period over,
//     Project over)
//   - Ageing in months, categorized for reporting
//   - Working units consumed since PO start (weekday-based with
//     partial-month prorata, capped at contract total)
//   - Amounts in EUR via a currency dimension merge
//
// The wrapped SQL query pulls from the project and quote dims plus the raw
// rfps table; the M transformations below handle the temporal expansion
// and business logic that is easier expressed row-by-row.
// ============================================================================
let
    Source = Query1(
        "select#(lf)" &
        "  p.po_sk,#(lf)" &
        "  p.project_id,#(lf)" &
        "  p.po_business_id,#(lf)" &
        "  p.start_date,#(lf)" &
        "  p.adjusted_end_date,#(lf)" &
        "  p.po_validation_date,#(lf)" &
        "  p.is_renewed,#(lf)" &
        "  p.po_created_at,#(lf)" &
        "  vw.entry_date,#(lf)" &
        "  vw.exit_date,#(lf)" &
        "  vw.status,#(lf)" &
        "  vw.is_active,#(lf)" &
        "  vw.is_active_in_platform,#(lf)" &
        "  vw.is_current,#(lf)" &
        "  p.work_unit_type,#(lf)" &
        "  p.work_units,#(lf)" &
        "  p.end_date,#(lf)" &
        "  f.currency as currency,#(lf)" &
        "  (SELECT DISTINCT i.currency #(lf)" &
        "   FROM warehouse.dim_invoice i #(lf)" &
        "   WHERE i.po_sk = p.po_sk AND i.is_client = true #(lf)" &
        "   LIMIT 1#(lf)" &
        "  ) as inv_currency,#(lf)" &
        "  f.client_price,#(lf)" &
        "  case #(lf)" &
        "      when p.po_sk like 'rfp_%' then r.created_at + interval 3 hours#(lf)" &
        "      else q.created_at#(lf)" &
        "  end as rfp_created_at#(lf)" &
        "from warehouse.dim_project as p#(lf)" &
        "left join warehouse.dim_project_financials as f #(lf)" &
        "  on p.po_sk = f.po_sk#(lf)" &
        "left join raw.rfps as r #(lf)" &
        "  on p.po_sk = concat('rfp_', r.id)#(lf)" &
        "left join warehouse.dim_quote as q #(lf)" &
        "  on p.po_sk = q.quote_id#(lf)" &
        "lateral view explode(p.validation_workflow) as vw#(lf)" &
        "where p.is_valid = true #(lf)" &
        "order by p.po_sk, vw.entry_date asc"
    ),

    // Normalise exit_date: NULL means "still open" → treat as today
    Custom1 = Table.TransformColumns(Source, {
        {"entry_date", each Date.From(_), type date},
        {"exit_date", each if _ = null then Date.From(DateTime.LocalNow()) else Date.From(_), type date}
    }),

    #"Changed Type1" = Table.TransformColumnTypes(Custom1, {
        {"start_date", type date}, {"adjusted_end_date", type date}, {"end_date", type date},
        {"rfp_created_at", type date}, {"po_validation_date", type date},
        {"work_units", type number}, {"client_price", type number}, {"po_created_at", type date}
    }),

    // Build the list of reporting dates per row:
    // all Fridays in [entry_date, exit_date] plus anchors (start, end, today, tomorrow).
    // List.Max guards against negative counts when exit_date < entry_date.
    Custom2 = Table.AddColumn(#"Changed Type1", "date_list", each
        let
            start = [entry_date],
            end = [exit_date],
            today = Date.From(DateTime.LocalNow()),
            tomorrow = Date.AddDays(today, 1),
            range = List.Dates(start, List.Max({Duration.Days(end - start) + 1, 0}), #duration(1,0,0,0)),
            friday = List.Select(range, each Date.DayOfWeek(_, Day.Monday) = 4),
            facts = List.Distinct(List.Combine({friday, {start, end, today, tomorrow}})),
            InRange = List.Select(facts, each _ >= start and _ <= end)
        in
            InRange
    ),

    #"Expanded date_list" = Table.ExpandListColumn(Custom2, "date_list"),

    #"Changed Type" = Table.TransformColumnTypes(#"Expanded date_list", {{"date_list", type date}}),

    // Cap the timeline: no more than 12 months ahead, no earlier than 2023-01-01
    #"Filter today + 12M" = Table.SelectRows(#"Changed Type", each
        [date_list] <= Date.AddMonths(Date.From(DateTime.LocalNow()), 12)
        and [date_list] >= #date(2023, 1, 1)
    ),

    // Flag: currently in "Pending renewal" status
    #"Add Pending Renewal" = Table.AddColumn(#"Filter today + 12M", "pending_renewal",
        each if [status] = "Pending renewal" then 1 else 0
    ),

    // Flag: PO exists but is not yet validated at this reporting date
    #"Add Pending PO" = Table.AddColumn(#"Add Pending Renewal", "pending_po", each
        if List.Contains({"Pending Client PO", "Pending Client PO & Legal docs", "Pending Legal docs", "Pending PO validation"}, [status])
            and [po_created_at] <> null
            and [date_list] >= [po_created_at]
            and [date_list] < (if [po_validation_date] = null
                               then Date.AddDays(Date.From(DateTime.LocalNow()), 1)
                               else [po_validation_date])
        then 1
        else 0
    ),

    // Flag: pending status extended past the adjusted end date
    #"Add Period Over" = Table.AddColumn(#"Add Pending PO", "period_over", each
        if ([pending_renewal] = 1 or [pending_po] = 1) and [date_list] <= [adjusted_end_date] then 0
        else if ([pending_renewal] = 1 or [pending_po] = 1) and [date_list] > [adjusted_end_date] then 1
        else null
    ),

    // Flag: project has ended (rules effective from 2025-04-01)
    #"Add Project Over" = Table.AddColumn(#"Add Period Over", "project_over", each
        if [date_list] < #date(2025, 4, 1) then null
        else if [status] = "No renewal information" and [is_renewed] <> null and [date_list] >= [adjusted_end_date] and ([period_over] = 0 or [period_over] = null) then 1
        else if [is_renewed] <> null and [date_list] >= [adjusted_end_date] and [date_list] < [exit_date] and ([period_over] = 0 or [period_over] = null) then 1
        else if [status] = "Project over" then 1
        else 0
    ),

    // Unified PO status label
    #"Add po_status" = Table.AddColumn(#"Add Project Over", "po_status",
        each if [pending_po] = 1 then "Pending PO"
             else if [pending_renewal] = 1 then "Pending renewal"
             else null
    ),

    // Ageing in months since start_date (pending) or since adjusted_end_date (project over)
    #"Add Ageing" = Table.AddColumn(
        #"Add po_status",
        "ageing_copil",
        each
            let
                curr = if [date_list] = null then Date.From(DateTime.LocalNow()) else Date.From([date_list])
            in
            if ([pending_renewal] = 1 or [pending_po] = 1) and [start_date] <> null then
                (Date.Year(curr) - Date.Year(Date.From([start_date]))) * 12
                + (Date.Month(curr) - Date.Month(Date.From([start_date])))
            else if [project_over] = 1 and [adjusted_end_date] <> null then
                (Date.Year(curr) - Date.Year(Date.From([adjusted_end_date]))) * 12
                + (Date.Month(curr) - Date.Month(Date.From([adjusted_end_date])))
            else
                null,
        Int64.Type
    ),

    // Categorical buckets for reporting: histo (6 buckets around zero) and ur_ageing (7 forward-only buckets)
    #"Add Ageing categories" = Table.AddColumn(
        #"Add Ageing",
        "ageing_copil_histo",
        each
            if [ageing_copil] = null then null
            else if [ageing_copil] < -1 then 1
            else if [ageing_copil] = -1 then 2
            else if [ageing_copil] = 0 then 3
            else if [ageing_copil] = 1 then 4
            else if [ageing_copil] = 2 then 5
            else 6,
        Int64.Type
    ),

    #"Add UR Ageing ID" = Table.AddColumn(
        #"Add Ageing categories",
        "ur_ageing_id",
        each
            let d = [ageing_copil] in
            if d = null then null
            else if d < 0 then null
            else if d = 0 then 0
            else if d = 1 then 1
            else if d >= 2 and d <= 3 then 2
            else if d >= 4 and d <= 6 then 3
            else if d >= 7 and d <= 9 then 4
            else if d >= 10 and d <= 12 then 5
            else 6,
        Int64.Type
    ),

    #"Changed Type Ageing" = Table.TransformColumnTypes(#"Add UR Ageing ID", {
        {"ageing_copil_histo", Int64.Type},
        {"ageing_copil", Int64.Type},
        {"ur_ageing_id", Int64.Type}
    }),

    // Flag: Friday snapshots + current-day snapshot
    #"Add is_friday" = Table.AddColumn(
        #"Changed Type Ageing",
        "is_friday",
        each if Date.DayOfWeek([date_list], Day.Monday) = 4 or [date_list] = Date.From(DateTime.LocalNow())
             then 1
             else 0,
        Int64.Type
    ),

    // Working units consumed since PO start (weekday-based, partial-month prorata, capped at contract total)
    #"Add working_units_since_start" = Table.AddColumn(
        #"Add is_friday",
        "working_units_since_start",
            (row) =>
                let
                    po_status      = row[po_status],
                    work_unit_type = row[work_unit_type],
                    work_units     = row[work_units],
                    start_date     = row[start_date],
                    end_date       = row[end_date],
                    date_list      = row[date_list],

                    WorkingDays = (d1 as date, d2 as date) as number =>
                        List.Count(
                            List.Select(
                                List.Dates(d1, Duration.Days(d2 - d1) + 1, #duration(1,0,0,0)),
                                (d) => Date.DayOfWeek(d, Day.Monday) < 5
                            )
                        ),

                    FirstDayOfMonth = (d as date) as date => Date.StartOfMonth(d),
                    LastDayOfMonth  = (d as date) as date => Date.EndOfMonth(d),
                    AddMonths       = (d as date, n as number) as date => Date.AddMonths(d, n),

                    EndOfPrevMonth = LastDayOfMonth( AddMonths(date_list, -1) ),

                    // Prorata of the first month (from start_date to end of that month)
                    ProrataStart =
                      if start_date = null then 0
                      else
                      Number.From( WorkingDays(start_date, LastDayOfMonth(start_date)) )
                      /
                      Number.From( WorkingDays(FirstDayOfMonth(start_date), LastDayOfMonth(start_date)) ),

                    // Prorata of the last month (only if it has already ended)
                    ProrataEnd =
                      if end_date = null then 0
                      else if end_date <= EndOfPrevMonth then
                        Number.From( WorkingDays(FirstDayOfMonth(end_date), end_date) )
                        /
                        Number.From( WorkingDays(FirstDayOfMonth(end_date), LastDayOfMonth(end_date)) )
                      else
                        0,

                    // Count of fully-elapsed months in between
                    FullMonths =
                      if start_date = null then 0
                      else
                      let
                        startFull = AddMonths(start_date, 1),
                        countM    =
                          ( Date.Year(EndOfPrevMonth) * 12 + Date.Month(EndOfPrevMonth) )
                          -
                          ( Date.Year(startFull) * 12 + Date.Month(startFull) ) + 1
                      in
                        if countM > 0 then countM else 0,

                    MonthUnits =
                      Number.Round( ProrataStart + ProrataEnd + FullMonths, 2 ),

                    // For Day/Hour contracts, count actual working days elapsed
                    WorkingDaysTotal =
                        if start_date = null then 0
                        else if start_date <= EndOfPrevMonth then
                            WorkingDays(start_date, EndOfPrevMonth)
                        else
                            0,

                    OtherUnits =
                      let
                        maxUnits =
                          if work_unit_type = "Day"  then work_units
                          else if work_unit_type = "Hour" then work_units / 8
                          else work_units
                      in
                        List.Min({WorkingDaysTotal, maxUnits}),

                    Result =
                       if List.Contains({"Pending renewal", "Pending PO"}, po_status) then
                        if work_unit_type = "Month" then
                          List.Min({MonthUnits, work_units})
                        else if work_unit_type = "Package" then null
                        else OtherUnits
                      else null
                in
                    Result
    ),

    #"Changed Type2" = Table.TransformColumnTypes(#"Add working_units_since_start", {{"working_units_since_start", Int64.Type}}),

    // Currency conversion to EUR
    #"Add final_currency" = Table.AddColumn(#"Changed Type2", "final_currency",
        each if [inv_currency] <> null then [inv_currency] else [currency]
    ),
    #"Add currency_date" = Table.AddColumn(#"Add final_currency", "currency_date",
        each if [rfp_created_at] <> null then [rfp_created_at] else [po_created_at], type date
    ),
    #"Merged Queries" = Table.NestedJoin(#"Add currency_date", {"final_currency", "currency_date"}, #"currency dim", {"currency", "date"}, "Dim Currency", JoinKind.LeftOuter),
    #"Expanded Dim Currency" = Table.ExpandTableColumn(#"Merged Queries", "Dim Currency", {"daily_rate", "monthly_rate"}, {"daily_rate", "monthly_rate"}),
    #"Add client_price_eur" = Table.AddColumn(#"Expanded Dim Currency", "client_price_eur",
        each Number.Round([client_price] / [monthly_rate], 2)
    ),
    #"Add UR amount eur" = Table.AddColumn(#"Add client_price_eur", "ur_amount_eur",
        each Number.Round(([working_units_since_start] * [client_price]) / [monthly_rate], 2)
    ),
    #"Changed Type5" = Table.TransformColumnTypes(#"Add UR amount eur", {
        {"is_friday", Int64.Type},
        {"working_units_since_start", Int64.Type},
        {"ur_amount_eur", type number},
        {"po_status", type text}
    })
in
    #"Changed Type5"
