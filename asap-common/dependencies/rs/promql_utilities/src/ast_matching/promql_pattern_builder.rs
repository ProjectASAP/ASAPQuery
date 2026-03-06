use serde_json::Value;
use std::collections::HashMap;
use tracing::debug;

/// PromQL Pattern Builder for creating PromQL-based patterns
/// This mirrors the Python PromQLPatternBuilder class
pub struct PromQLPatternBuilder;

impl PromQLPatternBuilder {
    /// Create a pattern for any node type
    pub fn any() -> Option<HashMap<String, Value>> {
        debug!("Creating wildcard pattern (any)");
        None
    }

    /// Create a binary operation pattern (BinaryExpr)
    pub fn binary_op(
        op: &str,
        left: Option<HashMap<String, Value>>,
        right: Option<HashMap<String, Value>>,
        collect_as: Option<&str>,
    ) -> Option<HashMap<String, Value>> {
        debug!("Creating binary operation pattern for op: {}", op);
        let mut pattern = HashMap::new();
        pattern.insert("type".to_string(), Value::String("BinaryExpr".to_string()));
        pattern.insert("op".to_string(), Value::String(op.to_string()));
        pattern.insert("left".to_string(), serde_json::to_value(left).unwrap());
        pattern.insert("right".to_string(), serde_json::to_value(right).unwrap());

        match collect_as {
            Some(collect) => pattern.insert(
                "_collect_as".to_string(),
                Value::String(collect.to_string()),
            ),
            None => pattern.insert("_collect_as".to_string(), Value::Null),
        };

        Some(pattern)
    }

    /// Create a metric pattern (VectorSelector)
    pub fn metric(
        name: Option<&str>,
        labels: Option<HashMap<String, String>>,
        at_modifier: Option<&str>,
        collect_as: Option<&str>,
    ) -> Option<HashMap<String, Value>> {
        debug!("Creating metric pattern for name: {:?}", name);
        let mut pattern = HashMap::new();
        pattern.insert(
            "type".to_string(),
            Value::String("VectorSelector".to_string()),
        );

        match name {
            Some(n) => pattern.insert("name".to_string(), Value::String(n.to_string())),
            None => pattern.insert("name".to_string(), Value::Null),
        };

        match labels {
            Some(l) => {
                let labels_value = serde_json::to_value(l).unwrap();
                pattern.insert("matchers".to_string(), labels_value)
            }
            None => pattern.insert("matchers".to_string(), Value::Null),
        };

        match at_modifier {
            Some(a) => pattern.insert("at".to_string(), Value::String(a.to_string())),
            None => pattern.insert("at".to_string(), Value::Null),
        };

        match collect_as {
            Some(c) => pattern.insert("_collect_as".to_string(), Value::String(c.to_string())),
            None => pattern.insert("_collect_as".to_string(), Value::Null),
        };

        Some(pattern)
    }

    /// Create a function pattern (Call)
    pub fn function(
        names: Vec<&str>,
        args: Vec<Option<HashMap<String, Value>>>,
        collect_as: Option<&str>,
        collect_args_as: Option<&str>,
    ) -> Option<HashMap<String, Value>> {
        debug!("Creating function pattern for names: {:?}", names);
        let mut pattern = HashMap::new();
        pattern.insert("type".to_string(), Value::String("Call".to_string()));

        let mut func = HashMap::new();
        func.insert("type".to_string(), Value::String("Function".to_string()));
        func.insert(
            "name".to_string(),
            Value::Array(names.iter().map(|n| Value::String(n.to_string())).collect()),
        );

        pattern.insert("func".to_string(), serde_json::to_value(func).unwrap());
        pattern.insert("args".to_string(), serde_json::to_value(args).unwrap());

        match collect_args_as {
            Some(c) => pattern.insert("_collect_args_as".to_string(), Value::String(c.to_string())),
            None => pattern.insert("_collect_args_as".to_string(), Value::Null),
        };

        match collect_as {
            Some(c) => pattern.insert("_collect_as".to_string(), Value::String(c.to_string())),
            None => pattern.insert("_collect_as".to_string(), Value::Null),
        };

        Some(pattern)
    }

    /// Create a subquery pattern (SubqueryExpr)
    pub fn subquery(
        expr: Option<HashMap<String, Value>>,
        duration: Option<&str>,
        collect_as: Option<&str>,
    ) -> Option<HashMap<String, Value>> {
        let mut pattern = HashMap::new();
        pattern.insert(
            "type".to_string(),
            Value::String("SubqueryExpr".to_string()),
        );
        pattern.insert("expr".to_string(), serde_json::to_value(expr).unwrap());

        match duration {
            Some(d) => pattern.insert("range".to_string(), Value::String(d.to_string())),
            None => pattern.insert("range".to_string(), Value::Null),
        };

        // Initialize step and offset as null, matching Python implementation
        pattern.insert("step".to_string(), Value::Null);
        pattern.insert("offset".to_string(), Value::Null);

        match collect_as {
            Some(c) => pattern.insert("_collect_as".to_string(), Value::String(c.to_string())),
            None => pattern.insert("_collect_as".to_string(), Value::Null),
        };

        Some(pattern)
    }

    /// Create a matrix selector pattern (MatrixSelector)
    pub fn matrix_selector(
        vector_selector: Option<HashMap<String, Value>>,
        range: Option<&str>,
        collect_as: Option<&str>,
    ) -> Option<HashMap<String, Value>> {
        let mut pattern = HashMap::new();
        pattern.insert(
            "type".to_string(),
            Value::String("MatrixSelector".to_string()),
        );
        pattern.insert(
            "vector_selector".to_string(),
            serde_json::to_value(vector_selector).unwrap(),
        );

        match range {
            Some(r) => pattern.insert("range".to_string(), Value::String(r.to_string())),
            None => pattern.insert("range".to_string(), Value::Null),
        };

        match collect_as {
            Some(c) => pattern.insert("_collect_as".to_string(), Value::String(c.to_string())),
            None => pattern.insert("_collect_as".to_string(), Value::Null),
        };

        Some(pattern)
    }

    /// Create an aggregation pattern (AggregateExpr)
    pub fn aggregation(
        ops: Vec<&str>,
        expr: Option<HashMap<String, Value>>,
        param: Option<HashMap<String, Value>>,
        by_labels: Option<Vec<&str>>,
        without_labels: Option<Vec<&str>>,
        collect_as: Option<&str>,
    ) -> Option<HashMap<String, Value>> {
        let mut pattern = HashMap::new();
        pattern.insert(
            "type".to_string(),
            Value::String("AggregateExpr".to_string()),
        );
        pattern.insert(
            "op".to_string(),
            Value::Array(ops.iter().map(|op| Value::String(op.to_string())).collect()),
        );
        pattern.insert("expr".to_string(), serde_json::to_value(expr).unwrap());

        match param {
            Some(p) => pattern.insert("param".to_string(), serde_json::to_value(p).unwrap()),
            None => pattern.insert("param".to_string(), Value::Null),
        };

        // Use single "modifier" field to match Python format
        let modifier_value = match (by_labels, without_labels) {
            (Some(_), None) => Value::String("by".to_string()),
            (None, Some(_)) => Value::String("without".to_string()),
            _ => Value::Null,
        };
        pattern.insert("modifier".to_string(), modifier_value);

        match collect_as {
            Some(c) => pattern.insert("_collect_as".to_string(), Value::String(c.to_string())),
            None => pattern.insert("_collect_as".to_string(), Value::Null),
        };

        Some(pattern)
    }

    /// Create a number literal pattern
    pub fn number(value: Option<f64>, collect_as: Option<&str>) -> Option<HashMap<String, Value>> {
        let mut pattern = HashMap::new();
        pattern.insert(
            "type".to_string(),
            Value::String("NumberLiteral".to_string()),
        );

        match value {
            Some(v) => pattern.insert(
                "value".to_string(),
                Value::Number(serde_json::Number::from_f64(v).unwrap()),
            ),
            None => pattern.insert("value".to_string(), Value::Null),
        };

        match collect_as {
            Some(c) => pattern.insert("_collect_as".to_string(), Value::String(c.to_string())),
            None => pattern.insert("_collect_as".to_string(), Value::Null),
        };

        Some(pattern)
    }
}
