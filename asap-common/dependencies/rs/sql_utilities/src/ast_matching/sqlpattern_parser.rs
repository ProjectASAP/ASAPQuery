use crate::sqlhelper::SQLSchema;
use crate::sqlhelper::{AggregationInfo, SQLQueryData, TimeInfo};
use sqlparser::ast::*;
use std::collections::HashSet;

use parse_datetime::parse_datetime;
use sqlparser::ast::Value::SingleQuotedString;

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
        // Convert CTE to subquery if present
        let query = self.cte_to_subquery(query);

        match &query.body.as_ref() {
            SetExpr::Select(select) => self.parse_select(select),
            _ => {
                println!("Not a SELECT statement");
                None
            }
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

    fn parse_select(&self, select: &Select) -> Option<SQLQueryData> {
        let (metric, has_subquery) = self.get_metric(select)?;

        let aggregation = self.get_aggregation(select)?;

        let group_bys = self.get_groupbys(select)?;

        if !has_subquery {
            let time_info = self.get_time_info(select, &metric)?;

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
                metric,
                labels: group_bys,
                time_info,
                subquery: None,
            })
        } else {
            // Parse subquery
            let subquery = match &select.from[0].relation {
                TableFactor::Derived { subquery, .. } => match subquery.body.as_ref() {
                    SetExpr::Select(inner_select) => {
                        let inner_aggregation = self.get_aggregation(inner_select)?;
                        let inner_group_bys = self.get_groupbys(inner_select)?;
                        let time_info = self.get_time_info(inner_select, &metric)?;

                        Some(Box::new(SQLQueryData {
                            aggregation_info: inner_aggregation,
                            metric: metric.clone(),
                            labels: inner_group_bys,
                            time_info,
                            subquery: None,
                        }))
                    }
                    _ => None,
                },
                _ => None,
            }?;

            Some(SQLQueryData {
                aggregation_info: aggregation,
                metric,
                labels: group_bys,
                time_info: TimeInfo::new("UNUSED".to_string(), -1.0, -1_f64),
                subquery: Some(subquery),
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

    fn get_aggregation(&self, select: &Select) -> Option<AggregationInfo> {
        if select.projection.len() != 1 {
            return None;
        }

        match &select.projection[0] {
            SelectItem::UnnamedExpr(Expr::Function(func))
            | SelectItem::ExprWithAlias {
                expr: Expr::Function(func),
                ..
            } => {
                let name = func.name.to_string().to_uppercase();

                let args = self.get_quantile_args(func);

                // Get the column being aggregated
                let col = match &func.args {
                    FunctionArguments::None => return None,
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
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        Expr::Identifier(ident),
                                    )) => ident.value.clone(),
                                    _ => return None,
                                }
                            }
                        } else if name == "PERCENTILE" {
                            // PERCENTILE(value, 95) - column is first argument
                            if func_args.args.is_empty() {
                                return None;
                            }
                            match &func_args.args[0] {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                                    ident,
                                ))) => ident.value.clone(),
                                _ => return None,
                            }
                        } else {
                            // For other aggregations - column is first argument
                            if func_args.args.is_empty() {
                                return None;
                            }
                            match &func_args.args[0] {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                                    ident,
                                ))) => ident.value.clone(),
                                _ => return None,
                            }
                        }
                    }
                };

                // Always store PERCENTILE as QUANTILE internally
                let normalized_name = if name == "PERCENTILE" {
                    "QUANTILE".to_string()
                } else {
                    name
                };

                Some(AggregationInfo::new(normalized_name, col, args))
            }
            _ => None,
        }
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
        let parsed_datetime = parse_datetime(datetime_str).ok()?;
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
            _ => {
                panic!("invalid time syntax {:?}", highlow);
            }
        }
    }

    fn get_time_info(&self, select: &Select, _table_name: &str) -> Option<TimeInfo> {
        let selection = select.selection.as_ref()?;

        match selection {
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                if *negated {
                    return None;
                }

                // Extract time column name
                let col_name = match expr.as_ref() {
                    Expr::Identifier(ident) => ident.value.clone(),
                    _ => return None,
                };

                let start = self.get_timestamp_from_between_highlow(low.as_ref())?;
                let end = self.get_timestamp_from_between_highlow(high.as_ref())?;

                let duration = end - start;

                Some(TimeInfo::new(col_name, start, duration))
            }
            _ => None,
        }
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
            }))) => parse_datetime(datetime_str).ok()?.timestamp().as_second() as f64,
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
    //             == (parse_datetime(datetime_str).ok()?.timestamp().as_second() as f64) => {}

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
