use crate::sqlhelper::SQLSchema;
use crate::sqlhelper::{
    AggregationInfo, OrderByItem, SQLBucketedCountIfOutput, SQLBucketedCountIfQueryData,
    SQLQueryData, TimeInfo,
};
use sqlparser::ast::*;
use std::collections::HashSet;

use parse_datetime::parse_datetime;
use sqlparser::ast::Value::SingleQuotedString;

/// One side of a half-open `time >= A AND time < B` range, carrying its
/// resolved timestamp (seconds).
enum TimeBound {
    /// `time >= ts` — inclusive lower bound.
    Lower(f64),
    /// `time < ts` — exclusive upper bound.
    Upper(f64),
}

/// Mirror a comparison operator for operand swaps (`A <= time` ≡ `time >= A`).
/// Only the operators relevant to half-open time ranges are mirrored; anything
/// else returns `None` and is rejected upstream.
fn mirror_operator(op: &BinaryOperator) -> Option<BinaryOperator> {
    match op {
        BinaryOperator::Lt => Some(BinaryOperator::Gt),
        BinaryOperator::LtEq => Some(BinaryOperator::GtEq),
        BinaryOperator::Gt => Some(BinaryOperator::Lt),
        BinaryOperator::GtEq => Some(BinaryOperator::LtEq),
        _ => None,
    }
}

pub struct SQLPatternParser {
    #[allow(dead_code)]
    schema: SQLSchema,
    query_evaluation_time: f64,
}

impl SQLPatternParser {
    pub fn new(schema: &SQLSchema, query_evaluation_time: f64) -> SQLPatternParser {
        Self {
            schema: schema.clone(),
            query_evaluation_time,
        }
    }

    /// Flatten an AND expression into a list of conjuncts.
    /// Example:
    ///   time BETWEEN ... AND ... AND collector = 'rrc00'
    /// becomes:
    ///   [time BETWEEN ... AND ..., collector = 'rrc00']
    fn flatten_and_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                Self::flatten_and_conjuncts(left, out);
                Self::flatten_and_conjuncts(right, out);
            }
            _ => out.push(expr),
        }
    }

    /// Try to parse one expression as a time predicate.
    fn get_time_info_from_expr(&self, expr: &Expr) -> Option<TimeInfo> {
        match expr {
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                if *negated {
                    return None;
                }

                let col_name = match expr.as_ref() {
                    Expr::Identifier(ident) => ident.value.clone(),
                    _ => return None,
                };

                let start = self.get_timestamp_from_between_highlow(low)?;
                let end = self.get_timestamp_from_between_highlow(high)?;
                let duration = end - start;

                Some(TimeInfo::new(col_name, start, duration))
            }

            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => self.get_time_info_from_half_open(left, right),

            _ => None,
        }
    }

    pub fn parse_bucketed_countif_query(
        &self,
        statements: &[Statement],
    ) -> Option<SQLBucketedCountIfQueryData> {
        if statements.len() != 1 {
            return None;
        }

        let query = match &statements[0] {
            Statement::Query(query) => query,
            _ => return None,
        };

        let order_by_items = self.parse_order_by_items(query)?;
        if query.limit_clause.is_some() {
            return None;
        }

        let query = self.cte_to_subquery(query);

        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            _ => return None,
        };

        self.parse_bucketed_countif_select(select, order_by_items)
    }

    fn parse_bucketed_countif_select(
        &self,
        select: &Select,
        order_by_items: Vec<OrderByItem>,
    ) -> Option<SQLBucketedCountIfQueryData> {
        let (metric, has_subquery) = self.get_metric(select)?;
        if has_subquery {
            return None;
        }

        if select.projection.len() < 2 {
            return None;
        }

        if select.distinct.is_some()
            || select.top.is_some()
            || select.into.is_some()
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.having.is_some()
            || !select.named_window.is_empty()
            || select.window_before_qualify
        {
            return None;
        }

        let time_info = self.get_time_info(select, &metric)?;
        let base_spatial_filter = self.get_spatial_filter(select);

        let (bucket_time_col, bucket_ms, bucket_alias) =
            self.parse_time_bucket_projection(&select.projection[0])?;

        if bucket_time_col != time_info.get_time_col_name() {
            return None;
        }

        let group_bys = self.get_groupbys(select)?;
        if group_bys.len() != 1 || !group_bys.contains(&bucket_alias) {
            return None;
        }

        for item in &order_by_items {
            if item.column != bucket_alias {
                return None;
            }
        }

        let mut outputs = Vec::new();
        for item in select.projection.iter().skip(1) {
            outputs.push(self.parse_countif_projection(item)?);
        }

        if outputs.is_empty() {
            return None;
        }

        Some(SQLBucketedCountIfQueryData {
            metric,
            time_info,
            bucket_alias,
            bucket_ms,
            base_spatial_filter,
            outputs,
            order_by: order_by_items,
        })
    }

    fn parse_time_bucket_projection(&self, item: &SelectItem) -> Option<(String, u64, String)> {
        let (expr, alias) = match item {
            SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
            _ => return None,
        };

        let func = match expr {
            Expr::Function(func) => func,
            _ => return None,
        };

        if !func
            .name
            .to_string()
            .eq_ignore_ascii_case("toStartOfInterval")
        {
            return None;
        }

        let args = match &func.args {
            FunctionArguments::List(args) => &args.args,
            _ => return None,
        };

        if args.len() != 2 {
            return None;
        }

        let time_col = match &args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                ident.value.clone()
            }
            _ => return None,
        };

        let interval_func = match &args[1] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(f))) => f,
            _ => return None,
        };

        if !interval_func
            .name
            .to_string()
            .eq_ignore_ascii_case("toIntervalMinute")
        {
            return None;
        }

        let interval_args = match &interval_func.args {
            FunctionArguments::List(args) => &args.args,
            _ => return None,
        };

        if interval_args.len() != 1 {
            return None;
        }

        let minutes = match &interval_args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan {
                value: Value::Number(n, _),
                ..
            }))) => n.parse::<u64>().ok()?,
            _ => return None,
        };

        Some((time_col, minutes * 60_000, alias))
    }

    fn parse_countif_projection(&self, item: &SelectItem) -> Option<SQLBucketedCountIfOutput> {
        let (expr, alias) = match item {
            SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
            _ => return None,
        };

        let func = match expr {
            Expr::Function(func) => func,
            _ => return None,
        };

        if !func.name.to_string().eq_ignore_ascii_case("countIf") {
            return None;
        }

        let args = match &func.args {
            FunctionArguments::List(args) => &args.args,
            _ => return None,
        };

        if args.len() != 1 {
            return None;
        }

        let cond = match &args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr,
            _ => return None,
        };

        let filter = self.parse_simple_equality_filter(cond)?;

        Some(SQLBucketedCountIfOutput { alias, filter })
    }

    fn parse_simple_equality_filter(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let col = match left.as_ref() {
                    Expr::Identifier(ident) => ident.value.clone(),
                    _ => return None,
                };

                let lit = match right.as_ref() {
                    Expr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(s),
                        ..
                    }) => s.clone(),
                    _ => return None,
                };

                // sqlparser already unescapes SingleQuotedString content, so any
                // embedded `'` must be re-escaped before we re-wrap it in quotes,
                // or the resulting filter string is malformed.
                Some(format!("{} = '{}'", col, lit.replace('\'', "''")))
            }
            _ => None,
        }
    }

    pub fn parse_query(&self, statements: &[Statement]) -> Option<SQLQueryData> {
        if statements.len() != 1 {
            println!("illegal query length");
            return None;
        }

        match &statements[0] {
            Statement::Query(query) => self.parse_query_node(query),
            _ => {
                println!("Not a query statement");
                None
            }
        }
    }

    fn parse_query_node(&self, query: &Query) -> Option<SQLQueryData> {
        // Parse ORDER BY / LIMIT before walking into the SELECT body. Both are properties
        // of the outer Query, not of the inner Select. Any unsupported sub-shape (positional
        // refs, expressions, OFFSET, NULLS FIRST/LAST, ClickHouse `ORDER BY ALL`, etc.)
        // bails the whole query rather than silently dropping it.
        let order_by_items = self.parse_order_by_items(query)?;
        let limit = self.parse_limit_value(query)?;

        // Convert CTE to subquery if present
        let query = self.cte_to_subquery(query);

        let mut data = match &query.body.as_ref() {
            SetExpr::Select(select) => self.parse_select(select)?,
            _ => {
                println!("Not a SELECT statement");
                return None;
            }
        };

        // ORDER BY columns must reference either the aggregate alias or a group-by key.
        // Anything else (e.g. `ORDER BY some_other_column`) is rejected to avoid the
        // engine returning an arbitrary order in cases where the user assumed the
        // column would resolve.
        for item in &order_by_items {
            let valid = data.aggregation_alias.as_deref() == Some(item.column.as_str())
                || data.labels.contains(&item.column);
            if !valid {
                return None;
            }
        }

        data.order_by = order_by_items;
        data.limit = limit;
        Some(data)
    }

    /// Convert `query.order_by` into a flat `Vec<OrderByItem>`.
    /// Returns `Some(vec![])` when no ORDER BY is present.
    /// Returns `None` for any unsupported shape (positional refs, expressions,
    /// `WITH FILL`, `NULLS FIRST/LAST`, ClickHouse `ORDER BY ALL`, `INTERPOLATE`).
    fn parse_order_by_items(&self, query: &Query) -> Option<Vec<OrderByItem>> {
        let order_by = match &query.order_by {
            None => return Some(Vec::new()),
            Some(ob) => ob,
        };
        if order_by.interpolate.is_some() {
            return None;
        }
        let exprs = match &order_by.kind {
            OrderByKind::Expressions(e) => e,
            // `ORDER BY ALL` (DuckDB / ClickHouse extension) is not supported.
            OrderByKind::All(_) => return None,
        };
        let mut items = Vec::with_capacity(exprs.len());
        for ob in exprs {
            if ob.with_fill.is_some() || ob.options.nulls_first.is_some() {
                return None;
            }
            let column = match &ob.expr {
                Expr::Identifier(ident) => ident.value.clone(),
                _ => return None,
            };
            // Default direction is ASC when neither ASC nor DESC is written.
            let ascending = ob.options.asc.unwrap_or(true);
            items.push(OrderByItem { column, ascending });
        }
        Some(items)
    }

    /// Convert `query.limit_clause` into an `Option<u64>`.
    /// Returns `Some(None)` when no LIMIT is present.
    /// Returns `None` for any unsupported shape (OFFSET, `LIMIT BY`, MySQL `LIMIT a, b`,
    /// non-literal expressions, `LIMIT ALL`).
    fn parse_limit_value(&self, query: &Query) -> Option<Option<u64>> {
        let clause = match &query.limit_clause {
            None => return Some(None),
            Some(c) => c,
        };
        let limit_expr = match clause {
            // MySQL-style `LIMIT a, b` (offset-comma-limit) is not supported.
            LimitClause::OffsetCommaLimit { .. } => return None,
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                if offset.is_some() || !limit_by.is_empty() {
                    return None;
                }
                match limit {
                    None => return Some(None), // `LIMIT ALL` or no LIMIT
                    Some(e) => e,
                }
            }
        };
        match limit_expr {
            Expr::Value(ValueWithSpan {
                value: Value::Number(n, _),
                ..
            }) => n.parse::<u64>().ok().map(Some),
            _ => None,
        }
    }

    fn cte_to_subquery(&self, query: &Query) -> Query {
        let mut query = query.clone();

        if let Some(with) = &query.with {
            if !with.cte_tables.is_empty() {
                let cte = &with.cte_tables[0];

                // Create a subquery from the CTE
                if let Some(new_body) = match &query.body.as_ref() {
                    SetExpr::Select(select) => {
                        let mut new_select = select.clone();
                        new_select.from = vec![TableWithJoins {
                            relation: TableFactor::Derived {
                                lateral: false,
                                subquery: Box::new(*(cte.query).clone()),
                                alias: None,
                            },
                            joins: vec![],
                        }];
                        Some(SetExpr::Select(Box::new(*new_select)))
                    }
                    _ => None,
                } {
                    query.body = Box::new(new_body);
                    query.with = None;
                }
            }
        }

        query
    }

    /// Find the single time predicate inside a flattened AND list.
    ///
    /// Supports both:
    ///   ts BETWEEN DATEADD(...) AND NOW()
    /// and:
    ///   ts >= start AND ts < end
    ///
    /// The second form becomes two separate conjuncts after flattening, so we
    /// must try pairs of conjuncts as a half-open time range.
    fn find_time_info_in_conjuncts(&self, conjuncts: &[&Expr]) -> Option<TimeInfo> {
        let mut matches = Vec::new();

        // Single-expression time predicates, e.g. BETWEEN.
        for expr in conjuncts {
            if let Some(time_info) = self.get_time_info_from_expr(expr) {
                matches.push(time_info);
            }
        }

        // Pair-expression half-open predicates:
        //   ts >= start AND ts < end
        for i in 0..conjuncts.len() {
            for j in (i + 1)..conjuncts.len() {
                if let Some(time_info) =
                    self.get_time_info_from_half_open(conjuncts[i], conjuncts[j])
                {
                    matches.push(time_info);
                }
            }
        }

        if matches.len() == 1 {
            matches.into_iter().next()
        } else {
            None
        }
    }

    /// Return true if an expression is one side of a half-open time range.
    fn is_time_comparison_side(&self, expr: &Expr) -> bool {
        self.parse_time_comparison(expr).is_some()
    }

    /// Return true if an expression can be parsed as the query's time predicate.
    fn is_time_predicate(&self, expr: &Expr) -> bool {
        self.get_time_info_from_expr(expr).is_some()
    }

    /// Extract metadata predicates from WHERE by removing the time predicate.
    /// The remaining predicates are returned as a SQL string for spatialFilter.
    fn get_spatial_filter(&self, select: &Select) -> Option<String> {
        let selection = select.selection.as_ref()?;

        let mut conjuncts = Vec::new();
        Self::flatten_and_conjuncts(selection, &mut conjuncts);

        let filters: Vec<String> = conjuncts
            .into_iter()
            .filter(|expr| !self.is_time_predicate(expr) && !self.is_time_comparison_side(expr))
            .map(|expr| expr.to_string())
            .collect();

        if filters.is_empty() {
            None
        } else {
            Some(filters.join(" AND "))
        }
    }

    fn parse_select(&self, select: &Select) -> Option<SQLQueryData> {
        let (metric, has_subquery) = self.get_metric(select)?;

        let (aggregation, aggregation_alias) = self.get_aggregation(select)?;

        let group_bys = self.get_groupbys(select)?;

        if !self.select_identifiers_subset_of(select, &group_bys) {
            return None;
        }

        if !has_subquery {
            let time_info = self.get_time_info(select, &metric)?;
            let spatial_filter = self.get_spatial_filter(select);

            // Check for unexpected fields
            if select.distinct.is_some()
                || select.top.is_some()
                || select.into.is_some()
                || !select.lateral_views.is_empty()
                || select.prewhere.is_some()
                || !select.cluster_by.is_empty()
                || !select.distribute_by.is_empty()
                || !select.sort_by.is_empty()
                || select.having.is_some()
                || !select.named_window.is_empty()
                || select.window_before_qualify
            {
                println!("Unexpected SELECT fields present");
                return None;
            }

            Some(SQLQueryData {
                aggregation_info: aggregation,
                spatial_filter,
                aggregation_alias,
                metric,
                labels: group_bys,
                time_info,
                subquery: None,
                order_by: Vec::new(),
                limit: None,
            })
        } else {
            // Parse subquery
            let subquery = match &select.from[0].relation {
                TableFactor::Derived { subquery, .. } => match subquery.body.as_ref() {
                    SetExpr::Select(inner_select) => {
                        let (inner_aggregation, inner_alias) =
                            self.get_aggregation(inner_select)?;
                        let inner_group_bys = self.get_groupbys(inner_select)?;
                        if !self.select_identifiers_subset_of(inner_select, &inner_group_bys) {
                            return None;
                        }
                        let time_info = self.get_time_info(inner_select, &metric)?;

                        let spatial_filter = self.get_spatial_filter(inner_select);

                        Some(Box::new(SQLQueryData {
                            aggregation_info: inner_aggregation,
                            spatial_filter,
                            aggregation_alias: inner_alias,
                            metric: metric.clone(),
                            labels: inner_group_bys,
                            time_info,
                            subquery: None,
                            order_by: Vec::new(),
                            limit: None,
                        }))
                    }
                    _ => None,
                },
                _ => None,
            }?;

            Some(SQLQueryData {
                aggregation_info: aggregation,
                spatial_filter: None,
                aggregation_alias,
                metric,
                labels: group_bys,
                time_info: TimeInfo::new("UNUSED".to_string(), -1.0, -1_f64),
                subquery: Some(subquery),
                order_by: Vec::new(),
                limit: None,
            })
        }
    }

    fn get_quantile_args(&self, func: &Function) -> Vec<String> {
        let name = func.name.to_string().to_uppercase();

        match (&func.args, name.as_str()) {
            (FunctionArguments::List(_), "QUANTILE") => {
                // ClickHouse parametric syntax: quantile(0.95)(column)
                // The quantile level is in func.parameters; func.args holds the column.
                if let FunctionArguments::List(params) = &func.parameters {
                    if !params.args.is_empty() {
                        let mut quantile_arg = Vec::new();
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) =
                            &params.args[0]
                        {
                            quantile_arg.push(value.value.to_string());
                        }
                        return quantile_arg;
                    }
                }

                // ASAP syntax: QUANTILE(0.95, column)
                // Both the quantile level and column are in func.args.
                let args = match &func.args {
                    FunctionArguments::List(a) => a,
                    _ => return Vec::new(),
                };
                let mut quantile_arg = Vec::new();
                match &args.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) => {
                        quantile_arg.push(value.value.to_string());
                        quantile_arg
                    }
                    _ => quantile_arg,
                }
            }
            (FunctionArguments::List(args), "PERCENTILE") => {
                let mut quantile_arg = Vec::new();

                // Convert PERCENTILE to QUANTILE format
                match &args.args[1] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) => {
                        let val_str = value.value.to_string();
                        if let Ok(percentile) = val_str.parse::<f64>() {
                            // Convert to quantile (0-1 range)
                            let quantile = if percentile > 1.0 {
                                percentile / 100.0
                            } else {
                                percentile
                            };
                            quantile_arg.push(quantile.to_string());
                        }
                        quantile_arg
                    }
                    _ => quantile_arg,
                }
            }
            _ => Vec::new(),
        }
    }

    /// Returns true iff every non-aggregate identifier in `select.projection` is
    /// also present in `group_bys`. Used to reject queries like
    /// `SELECT srcip, SUM(v) FROM t GROUP BY proto`, where standard SQL would
    /// require `srcip` to appear in the GROUP BY clause; without this check the
    /// pattern parser would silently drop `srcip` from the output.
    fn select_identifiers_subset_of(&self, select: &Select, group_bys: &HashSet<String>) -> bool {
        for item in &select.projection {
            let expr = match item {
                SelectItem::UnnamedExpr(expr) => expr,
                SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => continue,
            };
            if let Expr::Identifier(ident) = expr {
                if !group_bys.contains(&ident.value) {
                    return false;
                }
            }
        }
        true
    }

    fn get_aggregation(&self, select: &Select) -> Option<(AggregationInfo, Option<String>)> {
        // Find the (single) aggregate function in the projection list. Other
        // projection items must be plain column references — these are expected to
        // be group-by keys (e.g. `SELECT g1, g2, SUM(v) FROM t GROUP BY g1, g2`).
        // Anything else (multiple aggregates, computed expressions, literals, *)
        // is rejected since the structural pattern model only tracks one statistic.
        // Also captures the aggregate's alias if the SELECT writes `agg(v) AS <alias>`,
        // so `ORDER BY <alias>` can resolve later.
        let mut agg_func: Option<&Function> = None;
        let mut agg_alias: Option<String> = None;
        for item in &select.projection {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                _ => return None,
            };
            match expr {
                Expr::Function(f) => {
                    if agg_func.is_some() {
                        return None;
                    }
                    agg_func = Some(f);
                    agg_alias = alias;
                }
                Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {}
                _ => return None,
            }
        }
        let func = agg_func?;

        let name = func.name.to_string().to_uppercase();

        // DISTINCT handling. The structural model tracks at most one value column,
        // so we only support DISTINCT in its single-column COUNT form, which we
        // normalise to a cardinality aggregation:
        //   COUNT(DISTINCT col)          → name="CARDINALITY", value_column=col
        //   COUNT(DISTINCT col1, col2)   → rejected (compound-key distinct)
        //   COUNT(ALL col), COUNT(col)   → unchanged (plain COUNT)
        //   SUM/AVG/...(DISTINCT col)    → rejected (no sketch backs distinct-sum)
        let has_distinct = matches!(
            &func.args,
            FunctionArguments::List(list)
                if list.duplicate_treatment == Some(DuplicateTreatment::Distinct)
        );
        if has_distinct {
            if name != "COUNT" {
                return None;
            }
            if let FunctionArguments::List(list) = &func.args {
                if list.args.len() != 1 {
                    return None;
                }
            }
        }

        // ClickHouse's own distinct-count function family - same cardinality
        // semantics as COUNT(DISTINCT col), just a different spelling. Without
        // this, uniqExact(col) falls through to the generic "other aggregations"
        // branch below, gets treated as a plain aggregation named "UNIQEXACT",
        // and is rejected downstream as an illegal aggregation function - even
        // though the CARDINALITY path it needs already exists and works.
        let is_uniq_family = matches!(name.as_str(), "UNIQEXACT" | "UNIQ" | "UNIQCOMBINED");
        if is_uniq_family {
            if let FunctionArguments::List(list) = &func.args {
                if list.args.len() != 1 {
                    // Compound-key distinct (e.g. uniqExact(a, b)) isn't
                    // representable by the single-value-column model either -
                    // same limitation as COUNT(DISTINCT a, b) above.
                    return None;
                }
            }
        }

        let args = self.get_quantile_args(func);

        // Get the column being aggregated.
        //
        // ASAP's SQL planner originally required every aggregate to name a value
        // column, e.g. COUNT(v) or SUM(v). BGP Q1 uses COUNT() as an event count.
        // Treat COUNT() as a synthetic per-row count. The planner will map this
        // to a count sketch where each matching row contributes weight 1.
        let col = match &func.args {
            FunctionArguments::None => {
                if name == "COUNT" {
                    "__event_count__".to_string()
                } else {
                    return None;
                }
            }
            FunctionArguments::Subquery(_) => return None,
            FunctionArguments::List(func_args) => {
                if name == "QUANTILE" {
                    if let FunctionArguments::List(params) = &func.parameters {
                        if !params.args.is_empty() {
                            // ClickHouse parametric syntax: quantile(0.95)(column)
                            // Column is the sole argument in func.args.
                            match func_args.args.first() {
                                Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::Identifier(ident),
                                ))) => ident.value.clone(),
                                _ => return None,
                            }
                        } else {
                            return None;
                        }
                    } else {
                        // ASAP syntax: QUANTILE(0.95, value) - column is second argument
                        if func_args.args.len() < 2 {
                            return None;
                        }
                        match &func_args.args[1] {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                                ident,
                            ))) => ident.value.clone(),
                            _ => return None,
                        }
                    }
                } else if name == "PERCENTILE" {
                    // PERCENTILE(value, 95) - column is first argument
                    if func_args.args.is_empty() {
                        return None;
                    }
                    match &func_args.args[0] {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                            ident.value.clone()
                        }
                        _ => return None,
                    }
                } else {
                    // For other aggregations - column is first argument.
                    // Special case: COUNT() is parsed as an empty argument list.
                    // Treat COUNT() as an event count over a synthetic per-row value.
                    if func_args.args.is_empty() {
                        if name == "COUNT" {
                            "__event_count__".to_string()
                        } else {
                            return None;
                        }
                    } else {
                        match &func_args.args[0] {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                                ident,
                            ))) => ident.value.clone(),
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if name == "COUNT" => {
                                "__event_count__".to_string()
                            }
                            _ => return None,
                        }
                    }
                }
            }
        };

        // Normalisation:
        //   - PERCENTILE → QUANTILE (legacy alias).
        //   - COUNT(DISTINCT col) → CARDINALITY (validated above to be single-arg).
        //   - uniqExact/uniq/uniqCombined(col) → CARDINALITY (same, ClickHouse spelling).
        let normalized_name = if name == "PERCENTILE" {
            "QUANTILE".to_string()
        } else if has_distinct || is_uniq_family {
            "CARDINALITY".to_string()
        } else {
            name
        };

        Some((AggregationInfo::new(normalized_name, col, args), agg_alias))
    }

    fn get_metric(&self, select: &Select) -> Option<(String, bool)> {
        if select.from.is_empty() {
            return None;
        }

        match &select.from[0].relation {
            TableFactor::Table { name, .. } => {
                let metric = name.0.first()?.to_string();
                Some((metric, false))
            }
            TableFactor::Derived { subquery, .. } => match subquery.body.as_ref() {
                SetExpr::Select(inner_select) => {
                    if inner_select.from.is_empty() {
                        return None;
                    }
                    match &inner_select.from[0].relation {
                        TableFactor::Table { name, .. } => {
                            let metric = name.0.first()?.to_string();
                            Some((metric, true))
                        }
                        _ => None,
                    }
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn get_timestamp_from_datetime_str(datetime_str: &str) -> Option<f64> {
        // Treat SQL timestamp literals as UTC. Internally append a Z suffix before
        // parse_datetime so timezone-naive SQL literals match UTC-exported BGP data.
        let trimmed = datetime_str.trim();
        let utc_datetime = if trimmed.ends_with('Z') {
            trimmed.to_string()
        } else if trimmed.contains('T') {
            format!("{}Z", trimmed)
        } else {
            format!("{}Z", trimmed.replace(' ', "T"))
        };

        let parsed_datetime = parse_datetime(&utc_datetime).ok()?;
        Some(parsed_datetime.timestamp().as_second() as f64)
    }

    fn get_timestamp_from_between_highlow(&self, highlow: &Expr) -> Option<f64> {
        match highlow {
            Expr::Function(func) if func.name.to_string().to_uppercase() == "NOW" => {
                Some(self.query_evaluation_time)
            }
            Expr::Value(ValueWithSpan {
                value: SingleQuotedString(datetime_str),
                span: _,
            }) => Self::get_timestamp_from_datetime_str(datetime_str),
            Expr::Function(func) if func.name.to_string().to_uppercase() == "DATEADD" => {
                self.parse_dateadd(func)
            }

            // Elastic semantics requires CAST() for datetime strings.
            Expr::Cast { expr, .. } => match expr.as_ref() {
                Expr::Value(ValueWithSpan {
                    value: SingleQuotedString(datetime_str),
                    ..
                }) => Self::get_timestamp_from_datetime_str(datetime_str),
                _ => None,
            },

            // Unrecognized time syntax: return None so the query is treated as
            // unmatched (and forwarded / reported as unsupported) rather than
            // crashing the engine or planner.
            _ => None,
        }
    }

    fn get_time_info(&self, select: &Select, _table_name: &str) -> Option<TimeInfo> {
        let selection = select.selection.as_ref()?;

        let mut conjuncts = Vec::new();
        Self::flatten_and_conjuncts(selection, &mut conjuncts);

        self.find_time_info_in_conjuncts(&conjuncts)
    }

    /// Parse a `time >= A AND time < B` conjunction into `TimeInfo`.
    ///
    /// The two comparisons may appear in either order, and the time column may
    /// be on either side of each comparison. Both comparisons must reference the
    /// same column. Returns `None` unless there is exactly one `>=` lower bound
    /// and exactly one `<` upper bound.
    fn get_time_info_from_half_open(&self, left: &Expr, right: &Expr) -> Option<TimeInfo> {
        let (lcol, lbound) = self.parse_time_comparison(left)?;
        let (rcol, rbound) = self.parse_time_comparison(right)?;

        if lcol != rcol {
            return None;
        }

        let (start, end) = match (lbound, rbound) {
            (TimeBound::Lower(start), TimeBound::Upper(end)) => (start, end),
            (TimeBound::Upper(end), TimeBound::Lower(start)) => (start, end),
            // Two lower bounds or two upper bounds is not a valid range.
            _ => return None,
        };

        let duration = end - start;

        Some(TimeInfo::new(lcol, start, duration))
    }

    /// Parse a single comparison of the form `time >= <expr>` or `time < <expr>`
    /// (column on either side) into `(column_name, TimeBound)`.
    ///
    /// Strictly accepts only `>=` (lower) and `<` (upper); every other operator
    /// returns `None`.
    fn parse_time_comparison(&self, expr: &Expr) -> Option<(String, TimeBound)> {
        let Expr::BinaryOp { left, op, right } = expr else {
            return None;
        };

        // Normalize so the column identifier is on the left. If the column is on
        // the right instead, flip the operator to its mirror so the bound
        // classification below stays correct (e.g. `A <= time` ≡ `time >= A`).
        let (col_expr, op, ts_expr) = match (left.as_ref(), right.as_ref()) {
            (Expr::Identifier(_), _) => (left.as_ref(), op.clone(), right.as_ref()),
            (_, Expr::Identifier(_)) => (right.as_ref(), mirror_operator(op)?, left.as_ref()),
            _ => return None,
        };

        let col_name = match col_expr {
            Expr::Identifier(ident) => ident.value.clone(),
            _ => return None,
        };

        let ts = self.get_timestamp_from_between_highlow(ts_expr)?;

        let bound = match op {
            BinaryOperator::GtEq => TimeBound::Lower(ts),
            BinaryOperator::Lt => TimeBound::Upper(ts),
            // `>` and `<=` are intentionally rejected (see get_time_info).
            _ => return None,
        };

        Some((col_name, bound))
    }

    fn parse_dateadd(&self, func: &Function) -> Option<f64> {
        let args = match &func.args {
            FunctionArguments::List(args) => &args.args,
            _ => return None,
        };

        if args.len() != 3 {
            return None;
        }

        // First arg is time unit
        let time_unit = match &args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                ident.value.to_lowercase()
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan {
                value: SingleQuotedString(s),
                ..
            }))) => s.to_lowercase(),
            _ => return None,
        };

        // Second arg is the value
        let duration_to_add = match &args[1] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            })) => {
                println!("CORRECT MATCH EXPR!: {:?}", args[1]);
                match expr.as_ref() {
                    Expr::Value(ValueWithSpan {
                        value: Value::Number(n, _),
                        span: _,
                    }) => -n.parse::<i64>().ok()?,
                    _ => return None,
                }
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => match expr {
                Expr::Value(ValueWithSpan {
                    value: Value::Number(n, _),
                    span: _,
                }) => n.parse::<i64>().ok()?,
                _ => return None,
            },
            _ => {
                println!("DID NOT MATCH EXPR!: {:?}", args[1]);
                return None;
            }
        };

        let base_timestamp = match &args[2] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(func)))
                if func.name.to_string().to_uppercase() == "NOW" =>
            {
                self.query_evaluation_time
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan {
                value: SingleQuotedString(datetime_str),
                span: _,
            }))) => Self::get_timestamp_from_datetime_str(datetime_str)?,

            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Cast { expr, .. })) => {
                match expr.as_ref() {
                    Expr::Value(ValueWithSpan {
                        value: SingleQuotedString(datetime_str),
                        ..
                    }) => Self::get_timestamp_from_datetime_str(datetime_str)?,
                    _ => {
                        println!("Unsupported CAST expression in DATEADD");
                        return None;
                    }
                }
            }

            _ => {
                println!("time upper bound not calculating from present");
                return None;
            }
        };

        // Convert to seconds
        let multiplier = match time_unit.as_str() {
            "s" | "second" | "seconds" => 1.0,
            "m" | "minute" | "minutes" => 60.0,
            "h" | "hour" | "hours" => 3600.0,
            "d" | "day" | "days" => 86400.0,
            _ => return None,
        };

        Some(base_timestamp + (duration_to_add as f64) * multiplier)
    }

    // fn parse_dateadd_duration(&self, func: &Function, start: f64) -> Option<f64> {
    //     let args = match &func.args {
    //         FunctionArguments::List(args) => &args.args,
    //         _ => return None,
    //     };

    //     if args.len() != 3 {
    //         return None;
    //     }

    //     // First arg is time unit
    //     let time_unit = match &args[0] {
    //         FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
    //             ident.value.to_lowercase()
    //         }
    //         _ => return None,
    //     };

    //     // Second arg is the value
    //     let time_value = match &args[1] {
    //         FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::UnaryOp {
    //             op: UnaryOperator::Minus,
    //             expr,
    //         })) => {
    //             println!("CORRECT MATCH EXPR!: {:?}", args[1]);
    //             match expr.as_ref() {
    //                 Expr::Value(ValueWithSpan {
    //                     value: Value::Number(n, _),
    //                     span: _,
    //                 }) => n.parse::<i64>().ok()?,
    //                 _ => return None,
    //             }
    //         }
    //         FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => match expr {
    //             Expr::Value(ValueWithSpan {
    //                 value: Value::Number(n, _),
    //                 span: _,
    //             }) => n.parse::<i64>().ok()?,
    //             _ => return None,
    //         },
    //         _ => {
    //             println!("DID NOT MATCH EXPR!: {:?}", args[1]);
    //             return None;
    //         }
    //     };

    //     // Third arg should be NOW() or start
    //     // let printargs = &args[2];
    //     // println!("DATEADD ARGS: {printargs:?}");
    //     match &args[2] {
    //         FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(func)))
    //             if func.name.to_string().to_uppercase() == "NOW" => {}
    //         FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan {
    //             value: SingleQuotedString(datetime_str),
    //             span: _,
    //         }))) if start
    //             == (Self::get_timestamp_from_datetime_str(datetime_str)?) => {}

    //         _ => {
    //             println!("time upper bound not calculating from present");
    //             return None;
    //         }
    //     }

    //     // Convert to seconds
    //     let multiplier = match time_unit.as_str() {
    //         "s" | "second" | "seconds" => 1.0,
    //         "m" | "minute" | "minutes" => 60.0,
    //         "h" | "hour" | "hours" => 3600.0,
    //         "d" | "day" | "days" => 86400.0,
    //         _ => return None,
    //     };

    //     Some(time_value as f64 * multiplier)
    // }

    fn get_groupbys(&self, select: &Select) -> Option<HashSet<String>> {
        match &select.group_by {
            GroupByExpr::Expressions(exprs, mods) => {
                if !mods.is_empty() {
                    return None;
                }

                let mut group_bys = HashSet::new();

                for expr in exprs {
                    match expr {
                        Expr::Identifier(ident) => {
                            group_bys.insert(ident.value.clone());
                        }
                        _ => return None,
                    }
                }

                if group_bys.is_empty() {
                    None
                } else {
                    Some(group_bys)
                }
            }
            _ => None,
        }
    }
}
