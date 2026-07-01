use crate::sqlhelper::AggregationInfo;
use crate::sqlhelper::SQLQueryData;
use crate::sqlhelper::SQLSchema;
use crate::sqlhelper::TimeInfo;

use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    /// Dead: `query_info_to_pattern` no longer constructs this variant (#487). A query
    /// spanning exactly one scrape interval used to be forced into `Spatial` regardless
    /// of its GROUP BY, distinct from `Temporal*`/`SpatioTemporal`, which checked labels.
    /// That was never a real structural difference — `Spatial` was just `Temporal` with
    /// `range == scrape_interval` — so it's now classified by the same label-coverage
    /// check as everything else and lands in `TemporalGeneric`/`TemporalQuantile`
    /// (full GROUP BY) or `SpatioTemporal` (partial GROUP BY) like any other duration.
    /// Kept as a variant only because downstream `sql.rs` still matches on it; deleted
    /// once that's migrated to structural checks.
    Spatial,
    TemporalGeneric,
    TemporalQuantile,
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
}

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

        let mut sql_query = SQLQuery::new(Vec::new(), None, None);

        // Every subquery — outer or inner, whatever its duration — is classified purely
        // by aggregation + label coverage. Duration only gates validity above
        // (SpatialDurationSmall); it no longer decides the shape. A query that covers
        // all metadata columns is a plain temporal read (Quantile/Generic); anything
        // grouping by a subset of labels is SpatioTemporal, regardless of how long a
        // range it spans — a 1-interval query with a partial GROUP BY is exactly as
        // "spatiotemporal" as a 10-interval one.
        for (metric, aggregation_info, _scraped_intervals, labels, time_info) in query_data.iter() {
            let has_all_labels = self
                .schema
                .get_metadata_columns(metric)
                .map(|schema_metadata_columns| labels == schema_metadata_columns)
                .unwrap_or(true);

            let query_type = if !has_all_labels {
                QueryType::SpatioTemporal
            } else if aggregation_info.get_name() == "QUANTILE" {
                QueryType::TemporalQuantile
            } else {
                QueryType::TemporalGeneric
            };

            sql_query.add_subquery(
                query_type,
                aggregation_info.clone(),
                metric.clone(),
                labels.clone(),
                time_info.clone(),
            );
        }

        sql_query
    }
}
