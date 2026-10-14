//! RQE expression tree: self-contained RQE leaves combined by aggregations and
//! binary ops. Each JSON node is an object identified by its key:
//!
//! ```json
//! {"agg": "avg", "by": ["job"], "of":
//!   {"op": "/", "lhs": {"rqe": {...}}, "rhs": {"scalar": 2.0}}}
//! ```

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RqeCapability {
    Sum,
    Quantile,
    Cardinality,
    TopkByValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RqeLeaf {
    pub id: String,
    /// Metric name (PromQL) or table name (SQL).
    pub metric: String,
    pub capability: RqeCapability,
    pub lookback_ms: u64,
    pub grouping: Vec<String>,
    /// Quantile rank; set iff `capability` is `quantile`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phi: Option<f64>,
    /// Top-k size; set iff `capability` is `topk_by_value`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<u64>,
    /// SQL value column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RqeAggOp {
    Avg,
    Min,
    Max,
    Sum,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RqeBinaryOp {
    #[serde(rename = "+")]
    Add,
    #[serde(rename = "-")]
    Sub,
    #[serde(rename = "*")]
    Mul,
    #[serde(rename = "/")]
    Div,
}

// Serialized untagged (each variant's field names are the JSON keys);
// deserialized through `RawNode` so shape errors name the offending key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged, try_from = "RawNode")]
pub enum RqeExpr {
    Rqe {
        rqe: RqeLeaf,
    },
    Agg {
        agg: RqeAggOp,
        by: Vec<String>,
        of: Box<RqeExpr>,
    },
    Binary {
        op: RqeBinaryOp,
        lhs: Box<RqeExpr>,
        rhs: Box<RqeExpr>,
    },
    Scalar {
        scalar: f64,
    },
}

/// Union of all node keys; `TryFrom` picks the kind and rejects mixed keys.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawNode {
    rqe: Option<RqeLeaf>,
    agg: Option<RqeAggOp>,
    by: Option<Vec<String>>,
    of: Option<Box<RqeExpr>>,
    op: Option<RqeBinaryOp>,
    lhs: Option<Box<RqeExpr>>,
    rhs: Option<Box<RqeExpr>>,
    scalar: Option<f64>,
}

impl TryFrom<RawNode> for RqeExpr {
    type Error = String;

    fn try_from(raw: RawNode) -> Result<Self, String> {
        match raw {
            RawNode {
                rqe: Some(rqe),
                agg: None,
                by: None,
                of: None,
                op: None,
                lhs: None,
                rhs: None,
                scalar: None,
            } => Ok(RqeExpr::Rqe { rqe }),
            RawNode {
                rqe: None,
                agg: Some(agg),
                by: Some(by),
                of: Some(of),
                op: None,
                lhs: None,
                rhs: None,
                scalar: None,
            } => Ok(RqeExpr::Agg { agg, by, of }),
            RawNode {
                rqe: None,
                agg: None,
                by: None,
                of: None,
                op: Some(op),
                lhs: Some(lhs),
                rhs: Some(rhs),
                scalar: None,
            } => Ok(RqeExpr::Binary { op, lhs, rhs }),
            RawNode {
                rqe: None,
                agg: None,
                by: None,
                of: None,
                op: None,
                lhs: None,
                rhs: None,
                scalar: Some(scalar),
            } => Ok(RqeExpr::Scalar { scalar }),
            _ => Err("RQE expression node must be exactly one of \
                 {\"rqe\"}, {\"agg\", \"by\", \"of\"}, {\"op\", \"lhs\", \"rhs\"}, {\"scalar\"}"
                .to_string()),
        }
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum RqeExprError {
    #[error("RQE {id}: quantile requires phi in [0, 1], got {phi:?}")]
    InvalidPhi { id: String, phi: Option<f64> },
    #[error("RQE {id}: phi is only allowed for quantile")]
    UnexpectedPhi { id: String },
    #[error("RQE {id}: topk_by_value requires k > 0, got {k:?}")]
    InvalidK { id: String, k: Option<u64> },
    #[error("RQE {id}: k is only allowed for topk_by_value")]
    UnexpectedK { id: String },
    #[error("RQE {id}: topk_by_value is only allowed at the root")]
    TopkNotAtRoot { id: String },
    #[error("agg by {by:?} is not a subset of child labels {child:?}")]
    AggByNotSubset { by: Vec<String>, child: Vec<String> },
    #[error("binary op needs at least one non-scalar side")]
    BinaryBothScalar,
    #[error("binary op sides have different labels: {lhs:?} vs {rhs:?}")]
    BinaryLabelMismatch { lhs: Vec<String>, rhs: Vec<String> },
}

impl RqeExpr {
    /// Checks the whole tree; call once after loading.
    pub fn validate(&self) -> Result<(), RqeExprError> {
        self.validate_node(true)
    }

    fn validate_node(&self, is_root: bool) -> Result<(), RqeExprError> {
        match self {
            RqeExpr::Rqe { rqe } => rqe.validate(is_root),
            RqeExpr::Agg { by, of, .. } => {
                of.validate_node(false)?;
                let child = label_set(of.output_labels());
                if !label_set(by).is_subset(&child) {
                    return Err(RqeExprError::AggByNotSubset {
                        by: by.clone(),
                        child: of.output_labels().to_vec(),
                    });
                }
                Ok(())
            }
            RqeExpr::Binary { lhs, rhs, .. } => {
                lhs.validate_node(false)?;
                rhs.validate_node(false)?;
                match (lhs.is_scalar(), rhs.is_scalar()) {
                    (true, true) => Err(RqeExprError::BinaryBothScalar),
                    (false, false)
                        if label_set(lhs.output_labels()) != label_set(rhs.output_labels()) =>
                    {
                        Err(RqeExprError::BinaryLabelMismatch {
                            lhs: lhs.output_labels().to_vec(),
                            rhs: rhs.output_labels().to_vec(),
                        })
                    }
                    _ => Ok(()),
                }
            }
            RqeExpr::Scalar { .. } => Ok(()),
        }
    }

    /// Labels of this node's result: leaf grouping, agg `by`, the non-scalar
    /// side of a binary op (lhs if both), none for a scalar.
    pub fn output_labels(&self) -> &[String] {
        match self {
            RqeExpr::Rqe { rqe } => &rqe.grouping,
            RqeExpr::Agg { by, .. } => by,
            RqeExpr::Binary { lhs, rhs, .. } => {
                if lhs.is_scalar() {
                    rhs.output_labels()
                } else {
                    lhs.output_labels()
                }
            }
            RqeExpr::Scalar { .. } => &[],
        }
    }

    /// All RQE leaves, left to right.
    pub fn leaves(&self) -> Vec<&RqeLeaf> {
        let mut out = Vec::new();
        self.collect_leaves(&mut out);
        out
    }

    fn collect_leaves<'a>(&'a self, out: &mut Vec<&'a RqeLeaf>) {
        match self {
            RqeExpr::Rqe { rqe } => out.push(rqe),
            RqeExpr::Agg { of, .. } => of.collect_leaves(out),
            RqeExpr::Binary { lhs, rhs, .. } => {
                lhs.collect_leaves(out);
                rhs.collect_leaves(out);
            }
            RqeExpr::Scalar { .. } => {}
        }
    }

    fn is_scalar(&self) -> bool {
        matches!(self, RqeExpr::Scalar { .. })
    }
}

impl RqeLeaf {
    fn validate(&self, is_root: bool) -> Result<(), RqeExprError> {
        let id = || self.id.clone();
        match (self.capability, self.phi) {
            (RqeCapability::Quantile, Some(phi)) if (0.0..=1.0).contains(&phi) => {}
            (RqeCapability::Quantile, phi) => {
                return Err(RqeExprError::InvalidPhi { id: id(), phi })
            }
            (_, Some(_)) => return Err(RqeExprError::UnexpectedPhi { id: id() }),
            (_, None) => {}
        }
        match (self.capability, self.k) {
            (RqeCapability::TopkByValue, Some(k)) if k > 0 => {}
            (RqeCapability::TopkByValue, k) => return Err(RqeExprError::InvalidK { id: id(), k }),
            (_, Some(_)) => return Err(RqeExprError::UnexpectedK { id: id() }),
            (_, None) => {}
        }
        if self.capability == RqeCapability::TopkByValue && !is_root {
            return Err(RqeExprError::TopkNotAtRoot { id: id() });
        }
        Ok(())
    }
}

fn label_set(labels: &[String]) -> BTreeSet<&str> {
    labels.iter().map(String::as_str).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn leaf_json(id: &str, capability: &str, grouping: &[&str]) -> Value {
        json!({"rqe": {
            "id": id, "metric": "http_requests", "capability": capability,
            "lookback_ms": 60000, "grouping": grouping,
        }})
    }

    fn sum_leaf(id: &str, grouping: &[&str]) -> Value {
        leaf_json(id, "sum", grouping)
    }

    fn parse(v: Value) -> RqeExpr {
        serde_json::from_value(v).expect("valid JSON shape")
    }

    fn parse_err(v: Value) -> String {
        serde_json::from_value::<RqeExpr>(v)
            .expect_err("should not deserialize")
            .to_string()
    }

    fn assert_round_trip(v: Value) {
        let expr = parse(v.clone());
        assert_eq!(serde_json::to_value(&expr).unwrap(), v);
    }

    // --- JSON round trip ---

    #[test]
    fn round_trip_leaf_minimal() {
        assert_round_trip(sum_leaf("q0", &["job"]));
    }

    #[test]
    fn round_trip_leaf_all_optional_fields() {
        assert_round_trip(json!({"rqe": {
            "id": "q1", "metric": "events", "capability": "quantile",
            "lookback_ms": 300000, "grouping": [], "phi": 0.99, "value": "latency",
        }}));
        assert_round_trip(json!({"rqe": {
            "id": "q2", "metric": "events", "capability": "topk_by_value",
            "lookback_ms": 1000, "grouping": ["user_id"], "k": 10,
        }}));
        assert_round_trip(leaf_json("q3", "cardinality", &["job"]));
    }

    #[test]
    fn round_trip_agg() {
        for op in ["avg", "min", "max", "sum"] {
            assert_round_trip(
                json!({"agg": op, "by": ["job"], "of": sum_leaf("q", &["job", "instance"])}),
            );
        }
    }

    #[test]
    fn round_trip_binary() {
        for op in ["+", "-", "*", "/"] {
            assert_round_trip(
                json!({"op": op, "lhs": sum_leaf("a", &["job"]), "rhs": sum_leaf("b", &["job"])}),
            );
        }
    }

    #[test]
    fn round_trip_scalar() {
        assert_round_trip(json!({"scalar": 2.5}));
    }

    #[test]
    fn round_trip_nested() {
        assert_round_trip(json!({"agg": "avg", "by": ["job"], "of": {
            "op": "/", "lhs": sum_leaf("a", &["job", "instance"]), "rhs": sum_leaf("b", &["instance", "job"]),
        }}));
    }

    // --- deserialization errors ---

    #[test]
    fn rejects_unknown_capability() {
        for cap in ["rate", "topk_by_count"] {
            let err = parse_err(leaf_json("q", cap, &[]));
            assert!(err.contains(&format!("unknown variant `{cap}`")), "{err}");
        }
    }

    #[test]
    fn rejects_unknown_ops() {
        let err = parse_err(json!({"agg": "count", "by": [], "of": sum_leaf("q", &[])}));
        assert!(err.contains("unknown variant `count`"), "{err}");
        let err = parse_err(json!({"op": "%", "lhs": sum_leaf("a", &[]), "rhs": {"scalar": 1.0}}));
        assert!(err.contains("unknown variant `%`"), "{err}");
    }

    #[test]
    fn rejects_mixed_or_incomplete_node() {
        let mixed = json!({"scalar": 1.0, "op": "+"});
        let incomplete = json!({"agg": "sum", "of": sum_leaf("q", &[])});
        for v in [mixed, incomplete, json!({})] {
            assert!(parse_err(v).contains("must be exactly one of"));
        }
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(parse_err(json!({"scalar": 1.0, "extra": 1})).contains("unknown field `extra`"));
        let mut leaf = sum_leaf("q", &[]);
        leaf["rqe"]["filter"] = json!("x");
        assert!(parse_err(leaf).contains("unknown field `filter`"));
    }

    // --- validation: phi / k ---

    #[test]
    fn quantile_phi_rules() {
        let with_phi = |phi: Value| {
            let mut v = leaf_json("q", "quantile", &[]);
            v["rqe"]["phi"] = phi;
            parse(v).validate()
        };
        assert_eq!(with_phi(json!(0.5)), Ok(()));
        assert_eq!(with_phi(json!(0.0)), Ok(()));
        assert_eq!(with_phi(json!(1.0)), Ok(()));
        assert!(matches!(
            with_phi(json!(1.5)),
            Err(RqeExprError::InvalidPhi { .. })
        ));
        assert!(matches!(
            with_phi(json!(-0.1)),
            Err(RqeExprError::InvalidPhi { .. })
        ));
        assert!(matches!(
            parse(leaf_json("q", "quantile", &[])).validate(),
            Err(RqeExprError::InvalidPhi { phi: None, .. })
        ));
    }

    #[test]
    fn rejects_phi_on_non_quantile() {
        let mut v = sum_leaf("q", &[]);
        v["rqe"]["phi"] = json!(0.5);
        assert!(matches!(
            parse(v).validate(),
            Err(RqeExprError::UnexpectedPhi { .. })
        ));
    }

    #[test]
    fn topk_k_rules() {
        let with_k = |k: Value| {
            let mut v = leaf_json("q", "topk_by_value", &[]);
            v["rqe"]["k"] = k;
            parse(v).validate()
        };
        assert_eq!(with_k(json!(5)), Ok(()));
        assert!(matches!(
            with_k(json!(0)),
            Err(RqeExprError::InvalidK { k: Some(0), .. })
        ));
        assert!(matches!(
            parse(leaf_json("q", "topk_by_value", &[])).validate(),
            Err(RqeExprError::InvalidK { k: None, .. })
        ));
    }

    #[test]
    fn rejects_k_on_non_topk() {
        let mut v = leaf_json("q", "cardinality", &[]);
        v["rqe"]["k"] = json!(3);
        assert!(matches!(
            parse(v).validate(),
            Err(RqeExprError::UnexpectedK { .. })
        ));
    }

    // --- validation: top-k placement ---

    #[test]
    fn rejects_topk_below_root() {
        let mut topk = leaf_json("t", "topk_by_value", &["job"]);
        topk["rqe"]["k"] = json!(3);
        let agg = json!({"agg": "sum", "by": ["job"], "of": topk.clone()});
        let binary = json!({"op": "*", "lhs": topk, "rhs": {"scalar": 2.0}});
        for v in [agg, binary] {
            assert!(matches!(
                parse(v).validate(),
                Err(RqeExprError::TopkNotAtRoot { .. })
            ));
        }
    }

    // --- validation: agg ---

    #[test]
    fn agg_by_subset_rules() {
        let ok = json!({"agg": "max", "by": ["job"], "of": sum_leaf("q", &["instance", "job"])});
        assert_eq!(parse(ok).validate(), Ok(()));
        let empty_by = json!({"agg": "sum", "by": [], "of": sum_leaf("q", &["job"])});
        assert_eq!(parse(empty_by).validate(), Ok(()));
        let bad = json!({"agg": "max", "by": ["pod"], "of": sum_leaf("q", &["job"])});
        assert!(matches!(
            parse(bad).validate(),
            Err(RqeExprError::AggByNotSubset { .. })
        ));
    }

    #[test]
    fn agg_validates_child() {
        let v = json!({"agg": "sum", "by": [], "of": leaf_json("q", "quantile", &[])});
        assert!(matches!(
            parse(v).validate(),
            Err(RqeExprError::InvalidPhi { .. })
        ));
    }

    // --- validation: binary ---

    #[test]
    fn binary_label_set_rules() {
        let same_set = json!({"op": "-", "lhs": sum_leaf("a", &["job", "instance"]), "rhs": sum_leaf("b", &["instance", "job"])});
        assert_eq!(parse(same_set).validate(), Ok(()));
        let differ =
            json!({"op": "-", "lhs": sum_leaf("a", &["job"]), "rhs": sum_leaf("b", &["instance"])});
        assert!(matches!(
            parse(differ).validate(),
            Err(RqeExprError::BinaryLabelMismatch { .. })
        ));
    }

    #[test]
    fn binary_scalar_operand_rules() {
        let scalar_rhs =
            json!({"op": "/", "lhs": sum_leaf("a", &["job"]), "rhs": {"scalar": 1000.0}});
        assert_eq!(parse(scalar_rhs).validate(), Ok(()));
        let scalar_lhs = json!({"op": "-", "lhs": {"scalar": 1.0}, "rhs": sum_leaf("a", &["job"])});
        assert_eq!(parse(scalar_lhs).validate(), Ok(()));
        let both = json!({"op": "+", "lhs": {"scalar": 1.0}, "rhs": {"scalar": 2.0}});
        assert_eq!(parse(both).validate(), Err(RqeExprError::BinaryBothScalar));
    }

    #[test]
    fn binary_validates_children() {
        let bad_child = json!({"agg": "sum", "by": ["pod"], "of": sum_leaf("q", &["job"])});
        let v = json!({"op": "+", "lhs": sum_leaf("a", &["pod"]), "rhs": bad_child});
        assert!(matches!(
            parse(v).validate(),
            Err(RqeExprError::AggByNotSubset { .. })
        ));
    }

    // --- nested example and helpers ---

    fn nested_example() -> RqeExpr {
        parse(json!({"agg": "avg", "by": ["job"], "of": {
            "op": "/", "lhs": sum_leaf("a", &["job", "instance"]), "rhs": sum_leaf("b", &["instance", "job"]),
        }}))
    }

    #[test]
    fn nested_example_validates() {
        assert_eq!(nested_example().validate(), Ok(()));
    }

    #[test]
    fn leaves_in_order() {
        let ids: Vec<_> = nested_example()
            .leaves()
            .iter()
            .map(|l| l.id.clone())
            .collect();
        assert_eq!(ids, ["a", "b"]);
        assert!(parse(json!({"scalar": 1.0})).leaves().is_empty());
    }

    #[test]
    fn output_labels_per_kind() {
        let expr = nested_example();
        assert_eq!(expr.output_labels(), ["job"]);
        let RqeExpr::Agg { of, .. } = &expr else {
            unreachable!()
        };
        assert_eq!(of.output_labels(), ["job", "instance"]);
        let scalar_lhs =
            parse(json!({"op": "*", "lhs": {"scalar": 2.0}, "rhs": sum_leaf("a", &["pod"])}));
        assert_eq!(scalar_lhs.output_labels(), ["pod"]);
        assert!(parse(json!({"scalar": 2.0})).output_labels().is_empty());
    }
}
