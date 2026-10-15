//! Answers an RQE expression tree at one query time. A leaf reads the
//! aggregations its `rqe_configs` entry names (no query text is parsed), an
//! `agg` node regroups its child, and a `binary` node combines its sides the
//! way PromQL arithmetic does.

use super::promql::{combine_scalar, combine_vector_vector};
use super::{QueryExecutionError, QueryMetadata, QueryTimestamps, SimpleEngine};
use crate::data_model::RqeConfig;
use crate::engines::query_result::{InstantVectorElement, QueryResult};
use crate::engines::query_time_aggregation::{apply_instant_pipeline, pipeline_supports_labels};
use asap_types::query_config::{
    QueryTimeAggregation, QueryTimeAggregationOperator, QueryTimeGrouping, QueryTimeGroupingMode,
};
use asap_types::rqe_expr::{RqeAggOp, RqeBinaryOp, RqeCapability, RqeExpr, RqeLeaf};
use promql_parser::parser::token::{TokenType, T_ADD, T_DIV, T_MUL, T_SUB};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum RqeQueryError {
    /// The request is wrong: an invalid tree, an unknown RQE id or a bad time.
    #[error("{0}")]
    BadRequest(String),
    /// The request is valid but the engine could not answer it.
    #[error(transparent)]
    Execution(#[from] QueryExecutionError),
}

/// A node's result: an instant vector with its label names, or a literal.
enum RqeValue {
    Vector {
        labels: KeyByLabelNames,
        values: Vec<InstantVectorElement>,
    },
    Scalar(f64),
}

impl SimpleEngine {
    /// Evaluates `expr` at `time` (unix seconds) into an instant vector.
    ///
    /// Output labels are the root's labels (see `RqeExpr::output_labels`),
    /// never `__name__`: every leaf reads a range like `sum_over_time`, whose
    /// result Prometheus strips of its name. A leaf whose window holds no
    /// data yet contributes an empty vector.
    pub fn handle_rqe_query(
        &self,
        expr: &RqeExpr,
        time: f64,
    ) -> Result<(KeyByLabelNames, QueryResult), RqeQueryError> {
        expr.validate()
            .map_err(|error| RqeQueryError::BadRequest(error.to_string()))?;
        if !time.is_finite() || time < 0.0 {
            return Err(RqeQueryError::BadRequest(format!(
                "time must be a non-negative number of unix seconds, got {time}"
            )));
        }
        let leaves = expr.leaves();
        if leaves.is_empty() {
            return Err(RqeQueryError::BadRequest(
                "an RQE expression must read at least one RQE".to_string(),
            ));
        }
        // One snapshot of the configs, so every leaf reads the same plan.
        let configs: HashMap<&str, RqeConfig> = {
            let inference_config = self.inference_config.read().unwrap();
            leaves
                .iter()
                .map(|leaf| {
                    inference_config
                        .rqe_configs
                        .get(&leaf.id)
                        .map(|config| (leaf.id.as_str(), config.clone()))
                        .ok_or_else(|| {
                            RqeQueryError::BadRequest(format!("unknown RQE id {:?}", leaf.id))
                        })
                })
                .collect::<Result<_, _>>()?
        };

        let query_time = Self::convert_query_time_to_data_time(time);
        match self.evaluate_rqe_node(expr, &configs, query_time)? {
            RqeValue::Vector { labels, values } => {
                Ok((labels, QueryResult::vector(values, query_time)))
            }
            RqeValue::Scalar(_) => Err(RqeQueryError::BadRequest(
                "an RQE expression must evaluate to a vector".to_string(),
            )),
        }
    }

    fn evaluate_rqe_node(
        &self,
        expr: &RqeExpr,
        configs: &HashMap<&str, RqeConfig>,
        query_time: u64,
    ) -> Result<RqeValue, RqeQueryError> {
        match expr {
            RqeExpr::Rqe { rqe } => {
                let (labels, values) =
                    self.evaluate_rqe_leaf(rqe, &configs[rqe.id.as_str()], query_time)?;
                Ok(RqeValue::Vector { labels, values })
            }
            RqeExpr::Agg { agg, by, of } => {
                let RqeValue::Vector { labels, values } =
                    self.evaluate_rqe_node(of, configs, query_time)?
                else {
                    return Err(RqeQueryError::BadRequest(
                        "an agg node needs a vector operand, not a scalar".to_string(),
                    ));
                };
                let (labels, values) =
                    apply_instant_pipeline(labels, values, &[regroup_stage(*agg, by)])
                        .map_err(QueryExecutionError::Native)?;
                Ok(RqeValue::Vector { labels, values })
            }
            RqeExpr::Binary { op, lhs, rhs } => {
                let op = binary_token(*op);
                let lhs = self.evaluate_rqe_node(lhs, configs, query_time)?;
                let rhs = self.evaluate_rqe_node(rhs, configs, query_time)?;
                match (lhs, rhs) {
                    (
                        RqeValue::Vector {
                            labels,
                            values: lhs_values,
                        },
                        RqeValue::Vector {
                            labels: rhs_labels,
                            values: rhs_values,
                        },
                    ) => {
                        let values = combine_vector_vector(
                            lhs_values,
                            &labels.labels,
                            rhs_values,
                            &rhs_labels.labels,
                            &op,
                        )
                        .ok_or_else(|| {
                            RqeQueryError::BadRequest(format!(
                                "binary op sides have different labels: {:?} vs {:?}",
                                labels.labels, rhs_labels.labels
                            ))
                        })?;
                        Ok(RqeValue::Vector { labels, values })
                    }
                    (RqeValue::Vector { labels, values }, RqeValue::Scalar(scalar)) => {
                        Ok(RqeValue::Vector {
                            labels,
                            values: combine_scalar(values, scalar, &op, false),
                        })
                    }
                    (RqeValue::Scalar(scalar), RqeValue::Vector { labels, values }) => {
                        Ok(RqeValue::Vector {
                            labels,
                            values: combine_scalar(values, scalar, &op, true),
                        })
                    }
                    (RqeValue::Scalar(_), RqeValue::Scalar(_)) => Err(RqeQueryError::BadRequest(
                        "binary op needs at least one vector side".to_string(),
                    )),
                }
            }
            RqeExpr::Scalar { scalar } => Ok(RqeValue::Scalar(*scalar)),
        }
    }

    /// Reads `leaf` from the aggregations `config` names, over the window
    /// ending at `query_time` aligned as a PromQL instant query aligns it.
    /// A roll-up config reads at the aggregation's own grouping, then its
    /// query-time stages regroup to the leaf's. A top-k leaf ranks within
    /// each bucket of its aggregation's grouping, as PromQL `topk by` does,
    /// so that grouping must be the leaf's `topk_by`.
    fn evaluate_rqe_leaf(
        &self,
        leaf: &RqeLeaf,
        config: &RqeConfig,
        query_time: u64,
    ) -> Result<(KeyByLabelNames, Vec<InstantVectorElement>), RqeQueryError> {
        let failed =
            |error: String| QueryExecutionError::Native(format!("RQE {:?}: {error}", leaf.id));
        let agg_info = self
            .get_aggregation_id_info(&config.aggregations)
            .map_err(failed)?;
        let streaming_config = self.streaming_config.read().unwrap().clone();
        let value_config = streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .ok_or_else(|| {
                failed(format!(
                    "no streaming config for aggregation_id {}",
                    agg_info.aggregation_id_for_value
                ))
            })?;

        if leaf.capability == RqeCapability::TopkByValue {
            let buckets = KeyByLabelNames::new(leaf.topk_by.clone().unwrap_or_default());
            if value_config.grouping_labels != buckets {
                return Err(failed(format!(
                    "ranks within topk_by buckets {:?}, but its aggregation keeps a heap per {:?}",
                    buckets.labels, value_config.grouping_labels.labels
                ))
                .into());
            }
        }

        let end_timestamp = self.align_end_timestamp_promql(query_time);
        let start_timestamp = end_timestamp.checked_sub(leaf.lookback_ms).ok_or_else(|| {
            RqeQueryError::BadRequest(format!(
                "RQE {:?}: its {} ms lookback starts before the unix epoch",
                leaf.id, leaf.lookback_ms
            ))
        })?;

        let leaf_labels = KeyByLabelNames::new(leaf.grouping.clone());
        let read_labels = if config.query_time_aggregations.is_empty() {
            leaf_labels.clone()
        } else {
            let mut labels = value_config.grouping_labels.labels.clone();
            labels.extend(value_config.aggregated_labels.labels.iter().cloned());
            KeyByLabelNames::new(labels)
        };
        let (statistic_to_compute, query_kwargs) = leaf_statistic(leaf)?;
        let metadata = QueryMetadata {
            query_output_labels: read_labels,
            statistic_to_compute,
            query_kwargs,
            keep_metric_name: false,
        };
        let context = self
            .build_execution_context_tail(
                &value_config.metric,
                &QueryTimestamps {
                    start_timestamp,
                    end_timestamp,
                },
                metadata,
                agg_info,
                String::new(),
                query_time,
            )
            .map_err(failed)?;

        let Some((labels, result)) = self.execute_context_result(context, true, true)? else {
            return Ok((leaf_labels, Vec::new()));
        };
        let QueryResult::Vector(vector) = result else {
            return Err(failed("an instant read returned a range result".to_string()).into());
        };
        let (labels, values) = if config.query_time_aggregations.is_empty() {
            (labels, vector.values)
        } else {
            if !pipeline_supports_labels(&labels, &config.query_time_aggregations)
                .map_err(failed)?
            {
                return Err(failed(format!(
                    "query-time aggregations can't regroup labels {:?}",
                    labels.labels
                ))
                .into());
            }
            apply_instant_pipeline(labels, vector.values, &config.query_time_aggregations)
                .map_err(failed)?
        };
        if labels != leaf_labels {
            return Err(failed(format!(
                "read labels {:?}, not the leaf's grouping {:?}",
                labels.labels, leaf_labels.labels
            ))
            .into());
        }
        Ok((labels, values))
    }
}

/// The statistic and its kwargs a leaf's capability computes.
fn leaf_statistic(leaf: &RqeLeaf) -> Result<(Statistic, HashMap<String, String>), RqeQueryError> {
    let missing =
        |field: &str| RqeQueryError::BadRequest(format!("RQE {:?}: missing {field}", leaf.id));
    Ok(match leaf.capability {
        RqeCapability::Sum => (Statistic::Sum, HashMap::new()),
        RqeCapability::Cardinality => (Statistic::Cardinality, HashMap::new()),
        RqeCapability::Quantile => {
            let phi = leaf.phi.ok_or_else(|| missing("phi"))?;
            (
                Statistic::Quantile,
                HashMap::from([("quantile".to_string(), phi.to_string())]),
            )
        }
        RqeCapability::TopkByValue => {
            let k = leaf.k.ok_or_else(|| missing("k"))?;
            (
                Statistic::Topk,
                HashMap::from([("k".to_string(), k.to_string())]),
            )
        }
    })
}

/// The query-time stage that regroups a vector by `by` (all into one group
/// when `by` is empty).
fn regroup_stage(op: RqeAggOp, by: &[String]) -> QueryTimeAggregation {
    let operator = match op {
        RqeAggOp::Avg => QueryTimeAggregationOperator::Avg,
        RqeAggOp::Min => QueryTimeAggregationOperator::Min,
        RqeAggOp::Max => QueryTimeAggregationOperator::Max,
        RqeAggOp::Sum => QueryTimeAggregationOperator::Sum,
    };
    let mode = if by.is_empty() {
        QueryTimeGroupingMode::All
    } else {
        QueryTimeGroupingMode::By
    };
    QueryTimeAggregation {
        operator,
        grouping: QueryTimeGrouping {
            mode,
            labels: by.to_vec(),
        },
        parameter: None,
    }
}

fn binary_token(op: RqeBinaryOp) -> TokenType {
    TokenType::new(match op {
        RqeBinaryOp::Add => T_ADD,
        RqeBinaryOp::Sub => T_SUB,
        RqeBinaryOp::Mul => T_MUL,
        RqeBinaryOp::Div => T_DIV,
    })
}
