// Sketch-based query plan operators for DataFusion
//
// This module defines custom logical plan nodes for sketch-based query optimization.
// These operators support exploring different sketch-based execution strategies.
#![allow(deprecated)]

#[allow(deprecated)]
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DFSchema, DFSchemaRef, Result as DFResult};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::cmp::Ordering;
use std::collections::BTreeMap; // BTreeMap instead of HashMap (can derive Hash)
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

// ============================================================================
// TypedExpr - Expression with pre-resolved type
// ============================================================================

/// An expression paired with its pre-resolved data type.
///
/// This is used to preserve type information when expressions are passed through
/// plan nodes that may not have access to the original schema needed to resolve types.
/// For example, SummaryInfer's input is a SummaryInsert which may not include
/// the columns referenced in GROUP BY expressions (especially for Hydra strategy).
#[derive(Debug, Clone)]
pub struct TypedExpr {
    pub expr: Expr,
    pub data_type: DataType,
}

impl TypedExpr {
    pub fn new(expr: Expr, data_type: DataType) -> Self {
        Self { expr, data_type }
    }
}

// Manual trait implementations since Expr implements these traits
impl PartialEq for TypedExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr == other.expr && self.data_type == other.data_type
    }
}

impl Eq for TypedExpr {}

impl Hash for TypedExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.data_type.hash(state);
    }
}

// ============================================================================
// Sketch Types
// ============================================================================

/// Types of sketches/summaries supported for query processing
/// Also aliased as SummaryType for clarity (includes both sketches and exact aggregators)
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SketchType {
    // ========================================================================
    // Exact aggregators (non-sketch, single population)
    // ========================================================================
    Sum,      // Exact sum accumulator
    Increase, // Counter increase tracking
    MinMax,   // Min/max values

    MultipleSum,
    MultipleIncrease,
    MultipleMinMax,

    // ========================================================================
    // Set aggregators
    // ========================================================================
    SetAggregator,      // Exact set of group keys (HashSet-based)
    DeltaSetAggregator, // Set aggregation with separate key tracking

    // ========================================================================
    // COUNT DISTINCT sketches
    // ========================================================================
    HLL,         // HyperLogLog
    UltraLogLog, // UltraLogLog (improved HLL)
    HydraHLL,    // HyperLogLog with multi-population support

    // ========================================================================
    // Quantile sketches
    // ========================================================================
    KLL,      // KLL sketch
    TDigest,  // T-Digest
    HydraKLL, // KLL with multi-population support

    // ========================================================================
    // Heavy hitters / TOP K
    // ========================================================================
    SpaceSaving,   // Space-Saving algorithm
    FrequentItems, // Frequent items sketch

    // ========================================================================
    // Frequency estimation
    // ========================================================================
    CountMinSketch, // Count-Min Sketch
    CountSketch,    // Count Sketch

    // ========================================================================
    // General purpose
    // ========================================================================
    Sampling, // Reservoir sampling
}

/// Type alias for clarity - SummaryType includes both sketches and exact aggregators
pub type SummaryType = SketchType;

impl fmt::Display for SketchType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            // Exact aggregators
            SketchType::Sum => write!(f, "Sum"),
            SketchType::Increase => write!(f, "Increase"),
            SketchType::MinMax => write!(f, "MinMax"),
            SketchType::SetAggregator => write!(f, "SetAggregator"),
            SketchType::DeltaSetAggregator => write!(f, "DeltaSetAggregator"),
            SketchType::MultipleSum => write!(f, "MultipleSum"),
            SketchType::MultipleIncrease => write!(f, "MultipleIncrease"),
            SketchType::MultipleMinMax => write!(f, "MultipleMinMax"),
            // Sketches
            SketchType::HLL => write!(f, "HLL"),
            SketchType::UltraLogLog => write!(f, "UltraLogLog"),
            SketchType::HydraHLL => write!(f, "HydraHLL"),
            SketchType::KLL => write!(f, "KLL"),
            SketchType::TDigest => write!(f, "TDigest"),
            SketchType::HydraKLL => write!(f, "HydraKLL"),
            SketchType::SpaceSaving => write!(f, "SpaceSaving"),
            SketchType::FrequentItems => write!(f, "FrequentItems"),
            SketchType::CountMinSketch => write!(f, "CountMinSketch"),
            SketchType::CountSketch => write!(f, "CountSketch"),
            SketchType::Sampling => write!(f, "Sampling"),
        }
    }
}

impl SketchType {
    /// Check if this sketch type supports multi-population (Hydra-style)
    pub fn is_hydra(&self) -> bool {
        matches!(self, SketchType::HydraHLL | SketchType::HydraKLL)
    }

    /// Get the base sketch type (non-Hydra version)
    pub fn base_type(&self) -> SketchType {
        match self {
            SketchType::HydraHLL => SketchType::HLL,
            SketchType::HydraKLL => SketchType::KLL,
            other => other.clone(),
        }
    }
}

// ============================================================================
// Inference Operations
// ============================================================================

/// Operations that can be performed on sketches/summaries to extract results
/// Note: Uses simplified types (strings instead of Expr) for DataFusion integration
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum InferOperation {
    // ========================================================================
    // Exact aggregator extraction operations
    // ========================================================================
    /// Extract sum value from Sum/HydraSum accumulator
    ExtractSum,

    /// Extract count value from an accumulator
    ExtractCount,

    /// Extract minimum value from MinMax/HydraMinMax accumulator
    ExtractMin,

    /// Extract maximum value from MinMax/HydraMinMax accumulator
    ExtractMax,

    /// Extract increase value from Increase/HydraIncrease accumulator
    ExtractIncrease,

    /// Extract rate (increase / time_range) from Increase accumulator
    ExtractRate,

    // ========================================================================
    // Sketch operations
    // ========================================================================
    /// COUNT(DISTINCT column)
    CountDistinct,

    /// Quantile/percentile estimation
    /// Stores quantile as integer (0-10000) for 4 decimal places: 0.9500 = 9500
    Quantile(u16),

    /// Median (equivalent to Quantile(0.5))
    Median,

    /// TOP K items
    TopK(usize),

    /// Frequency-based COUNT(*) aggregation with GROUP BY
    /// Queries frequency sketch to get count for each group key
    FrequencyCount,

    /// Frequency-based SUM(column) aggregation with GROUP BY
    /// Queries frequency sketch to get sum for each group key
    FrequencySum,

    /// Frequency-based AVG(column) aggregation with GROUP BY
    /// Computed as SUM(column) / COUNT(*) using two frequency sketches
    FrequencyAvg,

    /// Frequency estimate for a specific value (stored as string)
    FrequencyEstimate(String),

    /// Get all frequent items above threshold (stored as integer, 0-10000)
    FrequentItems(u16),

    /// Enumerate set contents (for SetAggregator)
    /// Returns all unique values seen in the set
    EnumerateSet,
}

impl InferOperation {
    /// Create a Quantile operation from a float (0.0 to 1.0)
    pub fn quantile(p: f64) -> Self {
        InferOperation::Quantile((p * 10000.0).round() as u16)
    }

    /// Get the quantile value as f64
    pub fn quantile_value(&self) -> Option<f64> {
        match self {
            InferOperation::Quantile(p) => Some(*p as f64 / 10000.0),
            InferOperation::Median => Some(0.5),
            _ => None,
        }
    }

    /// Create a FrequentItems operation from a float threshold
    pub fn frequent_items(threshold: f64) -> Self {
        InferOperation::FrequentItems((threshold * 10000.0).round() as u16)
    }

    /// Get the threshold value as f64
    pub fn threshold_value(&self) -> Option<f64> {
        match self {
            InferOperation::FrequentItems(t) => Some(*t as f64 / 10000.0),
            _ => None,
        }
    }
}

impl fmt::Display for InferOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            // Exact aggregator extractions
            InferOperation::ExtractSum => write!(f, "EXTRACT_SUM"),
            InferOperation::ExtractCount => write!(f, "EXTRACT_COUNT"),
            InferOperation::ExtractMin => write!(f, "EXTRACT_MIN"),
            InferOperation::ExtractMax => write!(f, "EXTRACT_MAX"),
            InferOperation::ExtractIncrease => write!(f, "EXTRACT_INCREASE"),
            InferOperation::ExtractRate => write!(f, "EXTRACT_RATE"),
            // Sketch operations
            InferOperation::CountDistinct => write!(f, "COUNT_DISTINCT"),
            InferOperation::Quantile(p) => write!(f, "QUANTILE({:.4})", *p as f64 / 10000.0),
            InferOperation::Median => write!(f, "MEDIAN"),
            InferOperation::TopK(k) => write!(f, "TOPK({})", k),
            InferOperation::FrequencyCount => write!(f, "FREQ_COUNT"),
            InferOperation::FrequencySum => write!(f, "FREQ_SUM"),
            InferOperation::FrequencyAvg => write!(f, "FREQ_AVG"),
            InferOperation::FrequencyEstimate(value) => write!(f, "FREQ_EST({})", value),
            InferOperation::FrequentItems(threshold) => {
                write!(f, "FREQ_ITEMS({:.4})", *threshold as f64 / 10000.0)
            }
            InferOperation::EnumerateSet => write!(f, "ENUM_SET"),
        }
    }
}

// ============================================================================
// Grouping Strategy
// ============================================================================

/// Strategy for handling GROUP BY queries
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupingStrategy {
    /// One sketch per group (filter-based, computed separately)
    PerGroup,

    /// Single Hydra-style sketch containing all groups
    Hydra,

    /// No grouping (simple aggregation)
    None,
}

// ============================================================================
// Sketch Metadata
// ============================================================================

/// Metadata for identifying and loading sketches
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SketchMetadata {
    pub table_name: String,
    pub column_name: String,
    pub sketch_type: SketchType,
    pub filter_predicate: Option<String>,
    pub key_columns: Vec<String>, // For Hydra sketches
}

impl fmt::Display for SketchMetadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.table_name, self.column_name, self.sketch_type
        )?;
        if let Some(filter) = &self.filter_predicate {
            write!(f, " WHERE {}", filter)?;
        }
        if !self.key_columns.is_empty() {
            write!(f, " KEY BY [{}]", self.key_columns.join(", "))?;
        }
        Ok(())
    }
}

// ============================================================================
// Sketch Specification
// ============================================================================

/// Specification for a single sketch to create
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SketchSpec {
    pub value_column: Option<String>,
    pub sketch_type: SketchType,
    pub output_column_name: String, // e.g., "host_sketch", "cpu_sketch"
}

// ============================================================================
// SummaryInsert - Compute sketch from raw data
// ============================================================================

/// Logical plan node: Compute a sketch from raw data
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SummaryInsert {
    /// Input data source
    pub input: Arc<LogicalPlan>,

    /// Sketches to create (one or more)
    pub sketches: Vec<SketchSpec>,

    /// GROUP BY columns for per-group strategy (columns appear in output schema)
    /// Legacy field - use group_by_exprs for computed expressions
    pub group_by: Vec<String>,

    /// Key columns for Hydra strategy (columns embedded in sketch, NOT in output schema)
    /// Legacy field - use key_column_exprs for computed expressions
    pub key_columns: Vec<String>,

    /// GROUP BY expressions with pre-resolved types for per-group strategy
    /// When non-empty, takes precedence over group_by in compute_schema
    pub group_by_exprs: Vec<TypedExpr>,

    /// Key column expressions with pre-resolved types for Hydra strategy
    /// These are embedded in the sketch, not in output schema
    pub key_column_exprs: Vec<TypedExpr>,

    /// Optional parameters (e.g., HLL precision, KLL k value)
    /// Using BTreeMap instead of HashMap so it can derive Hash
    pub parameters: BTreeMap<String, String>,

    /// Cached output schema
    schema: DFSchemaRef,
}

impl SummaryInsert {
    /// Create a new SummaryInsert with multiple sketches
    pub fn new(input: Arc<LogicalPlan>, sketches: Vec<SketchSpec>) -> DFResult<Self> {
        if sketches.is_empty() {
            return Err(DataFusionError::Plan(
                "SummaryInsert requires at least one sketch".to_string(),
            ));
        }

        let schema = Self::compute_schema(&input, &sketches, &[], &[], &[])?;
        Ok(Self {
            input,
            sketches,
            group_by: vec![],
            key_columns: vec![],
            group_by_exprs: vec![],
            key_column_exprs: vec![],
            parameters: BTreeMap::new(),
            schema,
        })
    }

    /// Helper constructor for single sketch (backward compatibility)
    pub fn single(
        input: Arc<LogicalPlan>,
        value_column: Option<String>,
        sketch_type: SketchType,
    ) -> DFResult<Self> {
        let output_column_name = match &value_column {
            Some(col) => format!("{}_sketch", col),
            None => "value_sketch".to_string(),
        };

        Self::new(
            input,
            vec![SketchSpec {
                value_column,
                sketch_type,
                output_column_name,
            }],
        )
    }

    pub fn with_group_by(mut self, group_by: Vec<String>) -> DFResult<Self> {
        self.schema = Self::compute_schema(
            &self.input,
            &self.sketches,
            &group_by,
            &self.key_columns,
            &self.group_by_exprs,
        )?;
        self.group_by = group_by;
        Ok(self)
    }

    pub fn with_key_columns(mut self, key_columns: Vec<String>) -> DFResult<Self> {
        self.schema = Self::compute_schema(
            &self.input,
            &self.sketches,
            &self.group_by,
            &key_columns,
            &self.group_by_exprs,
        )?;
        self.key_columns = key_columns;
        Ok(self)
    }

    /// Set GROUP BY expressions with pre-resolved types (supports computed expressions)
    pub fn with_group_by_exprs(mut self, group_by_exprs: Vec<TypedExpr>) -> DFResult<Self> {
        self.schema = Self::compute_schema(
            &self.input,
            &self.sketches,
            &self.group_by,
            &self.key_columns,
            &group_by_exprs,
        )?;
        self.group_by_exprs = group_by_exprs;
        Ok(self)
    }

    /// Set key column expressions with pre-resolved types for Hydra strategy
    /// Note: These are embedded in the sketch, not in the output schema
    pub fn with_key_column_exprs(mut self, key_column_exprs: Vec<TypedExpr>) -> DFResult<Self> {
        // key_column_exprs don't affect output schema, but store them for later use
        self.key_column_exprs = key_column_exprs;
        Ok(self)
    }

    pub fn with_parameters(mut self, parameters: BTreeMap<String, String>) -> Self {
        self.parameters = parameters;
        self
    }

    /// Compute output schema based on grouping strategy
    fn compute_schema(
        input: &Arc<LogicalPlan>,
        sketches: &[SketchSpec],
        group_by: &[String],
        _key_columns: &[String],
        group_by_exprs: &[TypedExpr],
    ) -> DFResult<DFSchemaRef> {
        let input_schema = input.schema();
        let mut qualified_fields = Vec::new();

        // For per-group strategy: include group columns in output with their qualifications
        // This matches vanilla DataFusion Aggregate behavior
        //
        // Prefer group_by_exprs (TypedExpr) over group_by (String) if available
        if !group_by_exprs.is_empty() {
            // Use TypedExpr - supports computed expressions like date_part(), CASE, etc.
            for typed_expr in group_by_exprs {
                // For simple columns, use col.name as field name and col.relation as qualifier
                // For computed expressions, use schema_name() with no qualifier
                let (qualifier, field_name) = if let Expr::Column(col) = &typed_expr.expr {
                    (col.relation.clone(), col.name.clone())
                } else {
                    (None, typed_expr.expr.schema_name().to_string())
                };
                qualified_fields.push((
                    qualifier,
                    Arc::new(Field::new(&field_name, typed_expr.data_type.clone(), true)),
                ));
            }
        } else if !group_by.is_empty() {
            // Fallback to legacy string-based group_by
            for col_name in group_by {
                // Get both qualifier and field to preserve qualification
                let (qualifier, field) = input_schema
                    .qualified_field_with_unqualified_name(col_name)
                    .map_err(|e| {
                        DataFusionError::Plan(format!(
                            "Group column '{}' not found in input schema: {}",
                            col_name, e
                        ))
                    })?;
                qualified_fields.push((qualifier.cloned(), Arc::new(field.clone())));
            }
        }

        // For Hydra strategy: key columns are embedded in sketch, not in output
        // (no fields added here - neither key_columns nor key_column_exprs affect output)

        // Add sketch columns (Binary type, unqualified)
        // Create one column per sketch specification
        for sketch_spec in sketches {
            qualified_fields.push((
                None,
                Arc::new(Field::new(
                    &sketch_spec.output_column_name,
                    DataType::Binary,
                    false,
                )),
            ));
        }

        // Create DFSchema from qualified fields
        let schema = DFSchema::new_with_metadata(qualified_fields, Default::default())
            .map_err(|e| DataFusionError::Plan(format!("Failed to create schema: {}", e)))?;

        Ok(Arc::new(schema))
    }
}

impl PartialOrd for SummaryInsert {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Compare by sketches, then grouping, then parameters, then input
        match self.sketches.partial_cmp(&other.sketches) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.group_by.partial_cmp(&other.group_by) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.key_columns.partial_cmp(&other.key_columns) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.parameters.partial_cmp(&other.parameters) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        // Compare schemas by pointer (Arc comparison)
        Some(Arc::as_ptr(&self.schema).cmp(&Arc::as_ptr(&other.schema)))
    }
}

impl UserDefinedLogicalNodeCore for SummaryInsert {
    fn name(&self) -> &str {
        "SummaryInsert"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.sketches.len() == 1 {
            // Single sketch: show simplified format
            let sketch = &self.sketches[0];
            write!(f, "SummaryInsert: sketch_type={}", sketch.sketch_type)?;
            if let Some(col) = &sketch.value_column {
                write!(f, ", value_column={}", col)?;
            }
        } else {
            // Multiple sketches: show as array
            write!(f, "SummaryInsert: sketches=[")?;
            for (i, sketch) in self.sketches.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{{type={}", sketch.sketch_type)?;
                if let Some(col) = &sketch.value_column {
                    write!(f, ", column={}", col)?;
                }
                write!(f, "}}")?;
            }
            write!(f, "]")?;
        }
        if !self.group_by.is_empty() {
            write!(f, ", group_by=[{}]", self.group_by.join(", "))?;
        }
        if !self.key_columns.is_empty() {
            write!(f, ", key_columns=[{}]", self.key_columns.join(", "))?;
        }
        Ok(())
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        let input = Arc::new(inputs[0].clone());
        // Recompute schema with new input
        let schema = Self::compute_schema(
            &input,
            &self.sketches,
            &self.group_by,
            &self.key_columns,
            &self.group_by_exprs,
        )
        .unwrap_or_else(|_| self.schema.clone());

        Self {
            input,
            sketches: self.sketches.clone(),
            group_by: self.group_by.clone(),
            key_columns: self.key_columns.clone(),
            group_by_exprs: self.group_by_exprs.clone(),
            key_column_exprs: self.key_column_exprs.clone(),
            parameters: self.parameters.clone(),
            schema,
        }
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        Ok(self.from_template(&_exprs, &inputs))
    }
}

// ============================================================================
// SummaryRead - Load pre-computed sketch
// ============================================================================

/// Logical plan node: Load a pre-computed sketch
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SummaryRead {
    /// Metadata to identify the sketch
    pub metadata: SketchMetadata,

    /// Optional: Direct sketch ID if known
    pub sketch_id: Option<String>,

    /// Schema (placeholder for now)
    schema: DFSchemaRef,
}

impl SummaryRead {
    pub fn new(metadata: SketchMetadata, schema: DFSchemaRef) -> Self {
        Self {
            metadata,
            sketch_id: None,
            schema,
        }
    }

    pub fn with_sketch_id(mut self, sketch_id: String) -> Self {
        self.sketch_id = Some(sketch_id);
        self
    }
}

impl PartialOrd for SummaryRead {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.metadata.partial_cmp(&other.metadata) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.sketch_id.partial_cmp(&other.sketch_id) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        // DFSchemaRef is Arc<DFSchema>, and DFSchema likely doesn't implement PartialOrd
        // So we compare by pointer
        Some(Arc::as_ptr(&self.schema).cmp(&Arc::as_ptr(&other.schema)))
    }
}

impl UserDefinedLogicalNodeCore for SummaryRead {
    fn name(&self) -> &str {
        "SummaryRead"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![] // No inputs - reads from storage
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SummaryRead: {}", self.metadata)?;
        if let Some(id) = &self.sketch_id {
            write!(f, " [id={}]", id)?;
        }
        Ok(())
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        self.clone()
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        Ok(self.from_template(&_exprs, &inputs))
    }
}

// ============================================================================
// SummaryMerge - Merge multiple sketches
// ============================================================================

/// Logical plan node: Merge two or more sketches
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SummaryMerge {
    /// Left sketch source
    pub left: Arc<LogicalPlan>,

    /// Right sketch source
    pub right: Arc<LogicalPlan>,

    /// Sketch type (for validation)
    pub sketch_type: SketchType,
}

impl SummaryMerge {
    pub fn new(left: Arc<LogicalPlan>, right: Arc<LogicalPlan>, sketch_type: SketchType) -> Self {
        Self {
            left,
            right,
            sketch_type,
        }
    }
}

impl PartialOrd for SummaryMerge {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.sketch_type.partial_cmp(&other.sketch_type) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.left.partial_cmp(&other.left) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        self.right.partial_cmp(&other.right)
    }
}

impl UserDefinedLogicalNodeCore for SummaryMerge {
    fn name(&self) -> &str {
        "SummaryMerge"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        // Return left schema (should be compatible with right)
        self.left.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SummaryMerge: sketch_type={}", self.sketch_type)
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self {
            left: Arc::new(inputs[0].clone()),
            right: Arc::new(inputs[1].clone()),
            sketch_type: self.sketch_type.clone(),
        }
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        Ok(self.from_template(&_exprs, &inputs))
    }
}

// ============================================================================
// SummarySubtract - Subtract one sketch from another
// ============================================================================

/// Logical plan node: Subtract one sketch from another (for sliding windows)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SummarySubtract {
    /// Sketch to subtract FROM (minuend)
    pub minuend: Arc<LogicalPlan>,

    /// Sketch to subtract (subtrahend)
    pub subtrahend: Arc<LogicalPlan>,

    /// Sketch type (for validation)
    pub sketch_type: SketchType,
}

impl SummarySubtract {
    pub fn new(
        minuend: Arc<LogicalPlan>,
        subtrahend: Arc<LogicalPlan>,
        sketch_type: SketchType,
    ) -> Self {
        Self {
            minuend,
            subtrahend,
            sketch_type,
        }
    }
}

impl PartialOrd for SummarySubtract {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.sketch_type.partial_cmp(&other.sketch_type) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.minuend.partial_cmp(&other.minuend) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        self.subtrahend.partial_cmp(&other.subtrahend)
    }
}

impl UserDefinedLogicalNodeCore for SummarySubtract {
    fn name(&self) -> &str {
        "SummarySubtract"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.minuend.as_ref(), self.subtrahend.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.minuend.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SummarySubtract: sketch_type={}", self.sketch_type)
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self {
            minuend: Arc::new(inputs[0].clone()),
            subtrahend: Arc::new(inputs[1].clone()),
            sketch_type: self.sketch_type.clone(),
        }
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        Ok(self.from_template(&_exprs, &inputs))
    }
}

// ============================================================================
// SummaryInfer - Extract result from sketch
// ============================================================================

/// Logical plan node: Extract a result from a sketch
#[derive(Debug, Clone)]
pub struct SummaryInfer {
    /// Input sketch source
    pub input: Arc<LogicalPlan>,

    /// Optional second input for keys enumeration (multi-population accumulators).
    /// When present, SummaryInferExec deserializes the value sketch once per spatial group
    /// and queries it N times (once per sub-key from the keys input).
    pub keys_input: Option<Arc<LogicalPlan>>,

    /// Operations to perform on the sketch(es)
    /// For single sketch with multiple operations: operations map to sketch in order
    pub operations: Vec<InferOperation>,

    /// Output column names (one per operation)
    pub output_names: Vec<String>,

    /// Optional group key columns for Hydra sketches (column names, not full Expr)
    /// Legacy field - use group_key_exprs for computed expressions
    pub group_key_columns: Vec<String>,

    /// Optional qualifier for the group key columns (for Hydra sketches)
    pub group_key_qualifier: Option<String>,

    /// Group key expressions with pre-resolved types (supports computed expressions)
    /// When non-empty, takes precedence over group_key_columns in compute_schema
    pub group_key_exprs: Vec<TypedExpr>,

    /// Cached output schema
    schema: DFSchemaRef,
}

impl SummaryInfer {
    /// Create a new SummaryInfer with multiple operations
    pub fn new(
        input: Arc<LogicalPlan>,
        operations: Vec<InferOperation>,
        output_names: Vec<String>,
    ) -> DFResult<Self> {
        // Validate inputs
        if operations.is_empty() {
            return Err(DataFusionError::Plan(
                "SummaryInfer requires at least one operation".to_string(),
            ));
        }
        if operations.len() != output_names.len() {
            return Err(DataFusionError::Plan(format!(
                "SummaryInfer operations ({}) and output_names ({}) length mismatch",
                operations.len(),
                output_names.len()
            )));
        }

        let schema = Self::compute_schema(&input, &operations, &output_names, &[], &None, &[])?;
        Ok(Self {
            input,
            keys_input: None,
            operations,
            output_names,
            group_key_columns: vec![],
            group_key_qualifier: None,
            group_key_exprs: vec![],
            schema,
        })
    }

    /// Helper constructor for single operation (backward compatibility)
    pub fn single(
        input: Arc<LogicalPlan>,
        operation: InferOperation,
        output_name: String,
    ) -> DFResult<Self> {
        Self::new(input, vec![operation], vec![output_name])
    }

    /// Add group key columns for Hydra sketches (supports multiple columns)
    /// Legacy method - use with_group_key_exprs for computed expressions
    pub fn with_group_key_columns(
        mut self,
        group_key_columns: Vec<String>,
        qualifier: Option<String>,
    ) -> DFResult<Self> {
        self.schema = Self::compute_schema(
            &self.input,
            &self.operations,
            &self.output_names,
            &group_key_columns,
            &qualifier,
            &self.group_key_exprs,
        )?;
        self.group_key_columns = group_key_columns;
        self.group_key_qualifier = qualifier;
        Ok(self)
    }

    /// Set a second input for keys enumeration (multi-population accumulators).
    pub fn with_keys_input(mut self, keys_input: Arc<LogicalPlan>) -> Self {
        self.keys_input = Some(keys_input);
        self
    }

    /// Add group key expressions with pre-resolved types (supports computed expressions)
    pub fn with_group_key_exprs(
        mut self,
        group_key_exprs: Vec<TypedExpr>,
        qualifier: Option<String>,
    ) -> DFResult<Self> {
        self.schema = Self::compute_schema(
            &self.input,
            &self.operations,
            &self.output_names,
            &self.group_key_columns,
            &qualifier,
            &group_key_exprs,
        )?;
        self.group_key_exprs = group_key_exprs;
        self.group_key_qualifier = qualifier;
        Ok(self)
    }

    /// Compute output schema based on operations and grouping
    fn compute_schema(
        input: &Arc<LogicalPlan>,
        operations: &[InferOperation],
        output_names: &[String],
        group_key_columns: &[String],
        group_key_qualifier: &Option<String>,
        group_key_exprs: &[TypedExpr],
    ) -> DFResult<DFSchemaRef> {
        let input_schema = input.schema();
        let mut qualified_fields = Vec::new();

        // Add group columns to output with qualifications preserved
        // Prefer group_key_exprs (TypedExpr) over group_key_columns (String) if available
        if !group_key_exprs.is_empty() {
            // Use TypedExpr - supports computed expressions like date_part(), CASE, etc.
            // First: pass through input label columns not covered by group_key_exprs
            let expr_names: Vec<String> = group_key_exprs
                .iter()
                .filter_map(|te| {
                    if let Expr::Column(col) = &te.expr {
                        Some(col.name.clone())
                    } else {
                        None
                    }
                })
                .collect();
            for (qualifier, field) in input_schema.iter() {
                if field.name() != "sketch"
                    && !field.name().ends_with("_sketch")
                    && !expr_names.contains(&field.name().to_string())
                {
                    qualified_fields.push((qualifier.cloned(), field.clone()));
                }
            }
            // Then: add group key expression columns
            for typed_expr in group_key_exprs {
                // For simple columns, use col.name as field name and col.relation as qualifier
                // For computed expressions, use schema_name() with optional provided qualifier
                let (qualifier, field_name) = if let Expr::Column(col) = &typed_expr.expr {
                    (col.relation.clone(), col.name.clone())
                } else if let Some(qual) = group_key_qualifier {
                    (
                        Some(datafusion::common::TableReference::bare(qual.clone())),
                        typed_expr.expr.schema_name().to_string(),
                    )
                } else {
                    (None, typed_expr.expr.schema_name().to_string())
                };
                qualified_fields.push((
                    qualifier,
                    Arc::new(Field::new(&field_name, typed_expr.data_type.clone(), true)),
                ));
            }
        } else if !group_key_columns.is_empty() {
            // Fallback to legacy string-based group_key_columns
            // Hydra/self-keyed case: pass through input label columns first,
            // then add materialized group key columns from the accumulator.
            for (qualifier, field) in input_schema.iter() {
                if field.name() != "sketch"
                    && !field.name().ends_with("_sketch")
                    && !group_key_columns.contains(&field.name().to_string())
                {
                    qualified_fields.push((qualifier.cloned(), field.clone()));
                }
            }
            for key_col in group_key_columns {
                // Try to find it in the input schema first
                if let Ok((qualifier, field)) =
                    input_schema.qualified_field_with_unqualified_name(key_col)
                {
                    qualified_fields.push((qualifier.cloned(), Arc::new(field.clone())));
                } else if let Some(qual) = group_key_qualifier {
                    // Use provided qualifier if input schema doesn't have it
                    // This happens for Hydra where SummaryInsert doesn't output the group keys
                    let qualifier = Some(datafusion::common::TableReference::bare(qual.clone()));
                    qualified_fields.push((
                        qualifier,
                        Arc::new(Field::new(key_col, DataType::Utf8, false)),
                    ));
                } else {
                    // No qualifier available - use unqualified
                    qualified_fields
                        .push((None, Arc::new(Field::new(key_col, DataType::Utf8, false))));
                }
            }
        } else {
            // Per-group case: preserve group columns from input (non-sketch columns) with qualifications
            for (qualifier, field) in input_schema.iter() {
                if field.name() != "sketch" && !field.name().ends_with("_sketch") {
                    qualified_fields.push((qualifier.cloned(), field.clone()));
                }
            }
        }

        // Add result columns based on operation types (unqualified)
        for (operation, output_name) in operations.iter().zip(output_names.iter()) {
            let result_type = match operation {
                // Exact aggregator extractions - all return Float64
                InferOperation::ExtractSum => DataType::Float64,
                InferOperation::ExtractCount => DataType::Float64,
                InferOperation::ExtractMin => DataType::Float64,
                InferOperation::ExtractMax => DataType::Float64,
                InferOperation::ExtractIncrease => DataType::Float64,
                InferOperation::ExtractRate => DataType::Float64,
                // Sketch operations
                InferOperation::CountDistinct => DataType::UInt64,
                InferOperation::Quantile(_) | InferOperation::Median => DataType::Float64,
                InferOperation::TopK(_) => {
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                }
                InferOperation::FrequencyCount => DataType::Float64, // COUNT returns numeric
                InferOperation::FrequencySum => DataType::Float64,   // SUM returns numeric
                InferOperation::FrequencyAvg => DataType::Float64,   // AVG returns numeric
                InferOperation::FrequencyEstimate(_) => DataType::UInt64,
                InferOperation::FrequentItems(_) => {
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                }
                InferOperation::EnumerateSet => {
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                }
            };

            qualified_fields.push((None, Arc::new(Field::new(output_name, result_type, false))));
        }

        // Create DFSchema from qualified fields
        let schema = DFSchema::new_with_metadata(qualified_fields, Default::default())
            .map_err(|e| DataFusionError::Plan(format!("Failed to create schema: {}", e)))?;

        Ok(Arc::new(schema))
    }
}

impl PartialEq for SummaryInfer {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.keys_input == other.keys_input
            && self.operations == other.operations
            && self.output_names == other.output_names
            && self.group_key_columns == other.group_key_columns
            && self.group_key_qualifier == other.group_key_qualifier
            && self.schema == other.schema
    }
}

impl Eq for SummaryInfer {}

impl std::hash::Hash for SummaryInfer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.keys_input.hash(state);
        self.operations.hash(state);
        self.output_names.hash(state);
        self.group_key_columns.hash(state);
        self.group_key_qualifier.hash(state);
        self.schema.hash(state);
    }
}

impl PartialOrd for SummaryInfer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.operations.partial_cmp(&other.operations) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.output_names.partial_cmp(&other.output_names) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.group_key_columns.partial_cmp(&other.group_key_columns) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.keys_input.partial_cmp(&other.keys_input) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        // Compare schemas by pointer
        Some(Arc::as_ptr(&self.schema).cmp(&Arc::as_ptr(&other.schema)))
    }
}

impl UserDefinedLogicalNodeCore for SummaryInfer {
    fn name(&self) -> &str {
        "SummaryInfer"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        let mut inputs = vec![self.input.as_ref()];
        if let Some(ref keys_input) = self.keys_input {
            inputs.push(keys_input.as_ref());
        }
        inputs
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // No Expr types stored anymore - group_key_column is just a string
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.operations.len() == 1 {
            write!(
                f,
                "SummaryInfer: operation={}, output={}",
                self.operations[0], self.output_names[0]
            )?;
        } else {
            write!(f, "SummaryInfer: operations=[")?;
            for (i, op) in self.operations.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", op)?;
            }
            write!(f, "], outputs=[")?;
            for (i, name) in self.output_names.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", name)?;
            }
            write!(f, "]")?;
        }
        if !self.group_key_columns.is_empty() {
            write!(
                f,
                ", group_key_columns=[{}]",
                self.group_key_columns.join(", ")
            )?;
        }
        if self.keys_input.is_some() {
            write!(f, ", has_keys_input=true")?;
        }
        Ok(())
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        let input = Arc::new(inputs[0].clone());
        let keys_input = if inputs.len() > 1 {
            Some(Arc::new(inputs[1].clone()))
        } else {
            self.keys_input.clone()
        };
        // Recompute schema with new input
        let schema = Self::compute_schema(
            &input,
            &self.operations,
            &self.output_names,
            &self.group_key_columns,
            &self.group_key_qualifier,
            &self.group_key_exprs,
        )
        .unwrap_or_else(|_| self.schema.clone());

        Self {
            input,
            keys_input,
            operations: self.operations.clone(),
            output_names: self.output_names.clone(),
            group_key_columns: self.group_key_columns.clone(),
            group_key_qualifier: self.group_key_qualifier.clone(),
            group_key_exprs: self.group_key_exprs.clone(),
            schema,
        }
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        Ok(self.from_template(&exprs, &inputs))
    }
}

// ============================================================================
// PrecomputedSummaryRead - Read precomputed summaries from store
// ============================================================================

/// Logical plan node: Read precomputed summaries from a store
///
/// This is a leaf node that represents reading precomputed aggregates
/// (summaries) from a PrecomputedOutputStore. Used for OnlySpatial queries
/// where data has already been aggregated by a streaming engine.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrecomputedSummaryRead {
    /// Metric name being queried
    metric: String,

    /// Aggregation ID to query
    aggregation_id: u64,

    /// Start timestamp of the query range
    start_timestamp: u64,

    /// End timestamp of the query range
    end_timestamp: u64,

    /// Whether this is an exact query (sliding window) vs approximate (tumbling)
    is_exact_query: bool,

    /// Output label names (group by columns)
    output_labels: Vec<String>,

    /// Type of summary being read
    summary_type: SketchType,

    /// Cached output schema
    schema: DFSchemaRef,
}

impl PrecomputedSummaryRead {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metric: String,
        aggregation_id: u64,
        start_timestamp: u64,
        end_timestamp: u64,
        is_exact_query: bool,
        output_labels: Vec<String>,
        summary_type: SketchType,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            metric,
            aggregation_id,
            start_timestamp,
            end_timestamp,
            is_exact_query,
            output_labels,
            summary_type,
            schema,
        }
    }

    /// Create with auto-generated schema based on output_labels
    pub fn with_auto_schema(
        metric: String,
        aggregation_id: u64,
        start_timestamp: u64,
        end_timestamp: u64,
        is_exact_query: bool,
        output_labels: Vec<String>,
        summary_type: SketchType,
    ) -> DFResult<Self> {
        let schema = Self::compute_schema(&output_labels)?;
        Ok(Self::new(
            metric,
            aggregation_id,
            start_timestamp,
            end_timestamp,
            is_exact_query,
            output_labels,
            summary_type,
            schema,
        ))
    }

    /// Compute schema: [label columns (Utf8), sketch (Binary)]
    fn compute_schema(output_labels: &[String]) -> DFResult<DFSchemaRef> {
        let mut qualified_fields = Vec::new();

        // Add label columns (all Utf8, nullable)
        for label in output_labels {
            qualified_fields.push((None, Arc::new(Field::new(label, DataType::Utf8, true))));
        }

        // Add sketch column (Binary, not nullable)
        qualified_fields.push((
            None,
            Arc::new(Field::new("sketch", DataType::Binary, false)),
        ));

        let schema = DFSchema::new_with_metadata(qualified_fields, Default::default())
            .map_err(|e| DataFusionError::Plan(format!("Failed to create schema: {}", e)))?;

        Ok(Arc::new(schema))
    }

    // Getters
    pub fn metric(&self) -> &str {
        &self.metric
    }

    pub fn aggregation_id(&self) -> u64 {
        self.aggregation_id
    }

    pub fn start_timestamp(&self) -> u64 {
        self.start_timestamp
    }

    pub fn end_timestamp(&self) -> u64 {
        self.end_timestamp
    }

    pub fn is_exact_query(&self) -> bool {
        self.is_exact_query
    }

    pub fn output_labels(&self) -> &[String] {
        &self.output_labels
    }

    pub fn summary_type(&self) -> &SketchType {
        &self.summary_type
    }
}

impl PartialOrd for PrecomputedSummaryRead {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.metric.partial_cmp(&other.metric) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.aggregation_id.partial_cmp(&other.aggregation_id) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.start_timestamp.partial_cmp(&other.start_timestamp) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.end_timestamp.partial_cmp(&other.end_timestamp) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.is_exact_query.partial_cmp(&other.is_exact_query) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.output_labels.partial_cmp(&other.output_labels) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.summary_type.partial_cmp(&other.summary_type) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        Some(Arc::as_ptr(&self.schema).cmp(&Arc::as_ptr(&other.schema)))
    }
}

impl UserDefinedLogicalNodeCore for PrecomputedSummaryRead {
    fn name(&self) -> &str {
        "PrecomputedSummaryRead"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![] // Leaf node - no inputs
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PrecomputedSummaryRead: metric={}, agg_id={}, range=[{}, {}], exact={}, type={}, labels=[{}]",
            self.metric,
            self.aggregation_id,
            self.start_timestamp,
            self.end_timestamp,
            self.is_exact_query,
            self.summary_type,
            self.output_labels.join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        self.clone()
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(self.clone())
    }
}

// ============================================================================
// SummaryMergeMultiple - Merge multiple summaries by group key
// ============================================================================

/// Logical plan node: Merge multiple summaries with the same group key
///
/// Takes an input with multiple rows per group key (e.g., from multiple
/// precomputed buckets) and merges them into one summary per group key.
/// This is used when tumbling windows need to be merged for a query range.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SummaryMergeMultiple {
    /// Input plan (typically PrecomputedSummaryRead)
    input: Arc<LogicalPlan>,

    /// Columns to group by when merging
    group_by: Vec<String>,

    /// Column containing the sketch/summary data
    sketch_column: String,

    /// Type of summary being merged (for dispatch to correct merge logic)
    summary_type: SketchType,

    /// Cached output schema (same as input - merging reduces rows, not columns)
    schema: DFSchemaRef,
}

impl SummaryMergeMultiple {
    pub fn new(
        input: Arc<LogicalPlan>,
        group_by: Vec<String>,
        sketch_column: String,
        summary_type: SketchType,
    ) -> Self {
        // Schema is same as input (we reduce rows, not columns)
        let schema = input.schema().clone();
        Self {
            input,
            group_by,
            sketch_column,
            summary_type,
            schema,
        }
    }

    // Getters
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    pub fn group_by(&self) -> &[String] {
        &self.group_by
    }

    pub fn sketch_column(&self) -> &str {
        &self.sketch_column
    }

    pub fn summary_type(&self) -> &SketchType {
        &self.summary_type
    }
}

impl PartialOrd for SummaryMergeMultiple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.group_by.partial_cmp(&other.group_by) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.sketch_column.partial_cmp(&other.sketch_column) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.summary_type.partial_cmp(&other.summary_type) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => {}
            other => return other,
        }
        Some(Arc::as_ptr(&self.schema).cmp(&Arc::as_ptr(&other.schema)))
    }
}

impl UserDefinedLogicalNodeCore for SummaryMergeMultiple {
    fn name(&self) -> &str {
        "SummaryMergeMultiple"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SummaryMergeMultiple: group_by=[{}], sketch_column={}, type={}",
            self.group_by.join(", "),
            self.sketch_column,
            self.summary_type
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self {
            input: Arc::new(inputs[0].clone()),
            group_by: self.group_by.clone(),
            sketch_column: self.sketch_column.clone(),
            summary_type: self.summary_type.clone(),
            schema: inputs[0].schema().clone(),
        }
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        Ok(self.from_template(&exprs, &inputs))
    }
}
