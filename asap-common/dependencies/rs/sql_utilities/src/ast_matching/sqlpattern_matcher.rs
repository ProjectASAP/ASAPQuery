use crate::sqlhelper::AggregationInfo;
use crate::sqlhelper::SQLQueryData;
use crate::sqlhelper::SQLSchema;
use crate::sqlhelper::TimeInfo;

use std::collections::HashSet;

/// Every valid SQL query classifies as SpatioTemporal — query-type-agnostic,
/// so this is a single-variant enum. Kept (rather than removed outright) so
/// `SQLQuery.query_type: Vec<QueryType>` doesn't need reshaping here — see
/// #512 for that follow-up.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    SpatioTemporal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryError {
    InvalidAggregationLabel,
    InvalidTimeCol,
    InvalidValueCol,
    TemporalMissingLabels, // indistinguishable from too large scrape duration
    IllegalAggregationFn,
    SpatialDurationSmall,
    NestedQueryUnsupported,
    /// A two-layer query whose outer layer is not a query-time aggregation
    /// over its inner layer's output.
    InvalidNestedQuery,
    MissingTimeRange,
}

/// Aggregations an outer layer can apply to its inner layer's rows.
const QUERY_TIME_AGGREGATIONS: [&str; 6] = ["SUM", "COUNT", "AVG", "MIN", "MAX", "QUANTILE"];

#[derive(Debug)]
pub struct SQLQuery {
    pub query_type: Vec<QueryType>,
    pub query_data: Vec<SQLQueryData>,
    pub error: Option<QueryError>,
    pub msg: Option<String>,
}

impl SQLQuery {
    pub fn new(query_type: Vec<QueryType>, error: Option<QueryError>, msg: Option<String>) -> Self {
        Self {
            query_type,
            query_data: Vec::new(),
            error,
            msg,
        }
    }

    pub fn add_subquery(
        &mut self,
        query_type: QueryType,
        aggregation: AggregationInfo,
        metric: String,
        labels: HashSet<String>,
        time: TimeInfo,
    ) {
        self.query_type.push(query_type);

        let query_data = SQLQueryData {
            aggregation_info: aggregation,
            aggregation_alias: None,
            metric,
            labels,
            time_info: time,
            subquery: None,
            order_by: Vec::new(),
            limit: None,
        };

        self.query_data.push(query_data);
    }

    pub fn invalidate_query(&mut self, error: QueryError, msg: String) {
        self.error = Some(error);
        self.msg = Some(msg);
        self.query_type.clear();
    }

    pub fn is_valid(&self) -> bool {
        self.error.is_none()
    }

    /// The outer (spatial / single) query's data — always `query_data[0]`.
    pub fn outer_data(&self) -> Option<&SQLQueryData> {
        self.query_data.first()
    }

    /// The inner (temporal) query's data for nested queries — always `query_data[1]`.
    /// Only valid for `OneTemporalOneSpatial` patterns.
    pub fn inner_data(&self) -> Option<&SQLQueryData> {
        self.query_data.get(1)
    }
}

pub struct SQLPatternMatcher {
    schema: SQLSchema,
    scrape_interval: f64,
    legal_aggregations: HashSet<&'static str>,
}

impl SQLPatternMatcher {
    pub fn new(schema: SQLSchema, scrape_interval: f64) -> Self {
        let mut legal_aggregations = HashSet::new();
        legal_aggregations.insert("AVG");
        legal_aggregations.insert("SUM");
        legal_aggregations.insert("COUNT");
        legal_aggregations.insert("MIN");
        legal_aggregations.insert("MAX");
        legal_aggregations.insert("QUANTILE");
        // COUNT(DISTINCT col) is normalised by the parser to the aggregationname "CARDINALITY"
        legal_aggregations.insert("CARDINALITY");

        Self {
            schema,
            scrape_interval,
            legal_aggregations,
        }
    }

    pub fn is_valid_aggregation(&self, aggregation: &str) -> bool {
        self.legal_aggregations.contains(aggregation)
    }

    #[allow(clippy::type_complexity)]
    pub fn flatten_query_info(
        &self,
        query: &SQLQueryData,
    ) -> Result<Vec<(String, AggregationInfo, f64, HashSet<String>, TimeInfo)>, (QueryError, String)>
    {
        let mut query_data = Vec::new();
        let mut current_query = Some(query);
        let mut scraped_intervals = 0.0;

        while let Some(query) = current_query {
            if !self
                .schema
                .are_valid_metadata_columns(&query.metric, &query.labels)
            {
                if let Some(schema_metadata_columns) =
                    self.schema.get_metadata_columns(&query.metric)
                {
                    let illegal_columns: HashSet<_> =
                        query.labels.difference(schema_metadata_columns).collect();
                    println!("Returned QueryError::InvalidAggregationLabel");
                    return Err((
                        QueryError::InvalidAggregationLabel,
                        format!(
                            "attempt to aggregate by columns {:?}, which are not present for metric {}",
                            illegal_columns, query.metric
                        )
                    ));
                }
            }

            if !self.is_valid_aggregation(query.aggregation_info.get_name()) {
                println!("Returned QueryError::IllegalAggregationFn");

                return Err((
                    QueryError::IllegalAggregationFn,
                    format!(
                        "attempt to use illegal aggregation function {}",
                        query.aggregation_info.get_name()
                    ),
                ));
            }

            let time_info = &query.time_info;
            let time_column_name = time_info.get_time_col_name();

            if time_column_name != "UNUSED" {
                if let Some(schema_time_column) = self.schema.get_time_column(&query.metric) {
                    if time_column_name != schema_time_column {
                        println!("Returned QueryError::InvalidTimeCol: {time_column_name}");

                        return Err((
                            QueryError::InvalidTimeCol,
                            format!(
                                "Attempted to scrape from column [ {} ] instead of correct time column [ {} ]",
                                time_column_name, schema_time_column
                            )
                        ));
                    }
                }

                let value_column_name = query.aggregation_info.get_value_column_name();
                // `COUNT(DISTINCT col)` (normalised to "CARDINALITY") legitimately
                // targets metadata/label columns (e.g. `COUNT(DISTINCT dstip)`),
                // which the schema lists under metadata_columns rather than
                // value_columns. Accept either bucket for CARDINALITY; for all
                // other aggregations keep the strict value_columns-only check.
                let column_is_known = if query.aggregation_info.get_name() == "CARDINALITY" {
                    self.schema
                        .is_valid_value_column(&query.metric, value_column_name)
                        || self
                            .schema
                            .get_metadata_columns(&query.metric)
                            .is_some_and(|cols| cols.contains(value_column_name))
                } else {
                    self.schema
                        .is_valid_value_column(&query.metric, value_column_name)
                };
                if !column_is_known {
                    println!("Returned QueryError::InvalidValueCol");

                    return Err((
                        QueryError::InvalidValueCol,
                        format!("Incorrect value column name: {}", value_column_name),
                    ));
                }

                let scrape_duration = time_info.get_duration();
                scraped_intervals = scrape_duration / self.scrape_interval;

                if scraped_intervals < 1.0 {
                    println!("Returned QueryError::SpatialDurationSmall");

                    return Err((
                        QueryError::SpatialDurationSmall,
                        format!(
                            "scrape duration {} less than one interval {}",
                            scraped_intervals, self.scrape_interval
                        ),
                    ));
                }
            }

            query_data.push((
                query.metric.clone(),
                query.aggregation_info.clone(),
                scraped_intervals,
                query.labels.clone(),
                time_info.clone(),
            ));

            current_query = query.subquery.as_deref();
        }

        Ok(query_data)
    }

    pub fn query_info_to_pattern(&self, query_data: &SQLQueryData) -> SQLQuery {
        println!("SQLQueryData: {query_data:?}");
        let query_data = match self.flatten_query_info(query_data) {
            Ok(data) => data,
            Err((error, msg)) => {
                return SQLQuery::new(Vec::new(), Some(error), Some(msg));
            }
        };
        println!("flattened QueryData: {query_data:?}");

        if query_data.len() > 1 {
            println!("Returned QueryError::NestedQueryUnsupported");

            return SQLQuery::new(
                Vec::new(),
                Some(QueryError::NestedQueryUnsupported),
                Some(format!(
                    "nested SQL queries are not supported (n={})",
                    query_data.len()
                )),
            );
        }

        let mut sql_query = SQLQuery::new(Vec::new(), None, None);

        if let Some((metric, aggregation_info, scrape_duration, labels, time_info)) =
            query_data.first()
        {
            // scrape_duration is 0.0 only for the UNUSED-time-column marker
            // (the outer layer of a nested query, which the len() > 1 check
            // above already rejects) — not reachable for a flat query, since
            // any real time range short enough to compute a 0.0 (or < 1
            // scrape interval) duration already errors out above via
            // SpatialDurationSmall. Kept as an explicit error rather than
            // silently leaving the query unmatched, in case that invariant
            // ever changes.
            if *scrape_duration == 0.0 {
                println!("Returned QueryError::MissingTimeRange");

                return SQLQuery::new(
                    Vec::new(),
                    Some(QueryError::MissingTimeRange),
                    Some("query has no resolvable time range".to_string()),
                );
            }

            // Every valid query classifies as SpatioTemporal
            sql_query.add_subquery(
                QueryType::SpatioTemporal,
                aggregation_info.clone(),
                metric.clone(),
                labels.clone(),
                time_info.clone(),
            );
        }

        sql_query
    }

    /// Matches a two-layer query whose outer layer aggregates its inner
    /// layer's rows, e.g. `SELECT job, SUM(v) FROM (<inner>) GROUP BY job`.
    /// Returns the inner layer's pattern; the outer layer runs at query time.
    pub fn nested_query_to_pattern(&self, query: &SQLQueryData) -> SQLQuery {
        let invalid = |msg: String| {
            SQLQuery::new(Vec::new(), Some(QueryError::InvalidNestedQuery), Some(msg))
        };
        let Some(inner) = query.subquery.as_deref() else {
            return invalid("query has no inner layer".to_string());
        };

        if !query.order_by.is_empty() || query.limit.is_some() {
            return invalid("ORDER BY / LIMIT on the outer layer is not supported".to_string());
        }

        let outer_aggregation = query.aggregation_info.get_name();
        if !QUERY_TIME_AGGREGATIONS.contains(&outer_aggregation) {
            return invalid(format!(
                "outer aggregation {outer_aggregation} cannot run over the inner layer"
            ));
        }

        let outer_column = query.aggregation_info.get_value_column_name();
        if inner.aggregation_alias.as_deref() != Some(outer_column) {
            return invalid(format!(
                "outer aggregation reads {outer_column}, which is not the inner aggregate's alias"
            ));
        }

        if !query.labels.is_subset(&inner.labels) {
            let missing: HashSet<_> = query.labels.difference(&inner.labels).collect();
            return invalid(format!(
                "outer layer groups by {missing:?}, which the inner layer does not output"
            ));
        }

        self.query_info_to_pattern(inner)
    }
}
