use promql_utilities::query_logics::enums::AggregationType;

#[derive(Debug, Clone, Copy)]
pub struct SketchProperties {
    /// Two instances can be combined into one representing the union.
    pub mergeable: bool,
    /// Element-wise difference is defined; enables the Subtract query method (tumbling only).
    pub subtractable: bool,
    /// One deployed instance handles multiple label-group keys simultaneously.
    pub subpopulation_aware: bool,
}

pub fn sketch_properties(t: AggregationType) -> SketchProperties {
    let p = |me, su, sp| SketchProperties {
        mergeable: me,
        subtractable: su,
        subpopulation_aware: sp,
    };
    match t {
        AggregationType::Sum => p(true, true, false),
        AggregationType::Increase => p(true, false, false),
        AggregationType::MinMax => p(true, false, false),
        AggregationType::DatasketchesKLL => p(true, false, false),
        AggregationType::MultipleSum => p(true, true, true),
        AggregationType::MultipleIncrease => p(true, false, true),
        AggregationType::MultipleMinMax => p(true, false, true),
        AggregationType::HydraKLL => p(true, false, true),
        AggregationType::CountMinSketch => p(true, true, true),
        // ponytail: heap top-k lists don't compose across windows; CMS cells do but the
        // combined type requires the heap, so neither merging nor subtracting is safe here.
        AggregationType::CountMinSketchWithHeap => p(false, false, true),
        AggregationType::SetAggregator | AggregationType::DeltaSetAggregator => {
            p(true, false, false)
        }
        AggregationType::HLL => p(true, false, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cms_is_mergeable_subtractable_subpop_aware() {
        let p = sketch_properties(AggregationType::CountMinSketch);
        assert!(p.mergeable && p.subtractable && p.subpopulation_aware);
    }

    #[test]
    fn cms_with_heap_not_mergeable_not_subtractable() {
        let p = sketch_properties(AggregationType::CountMinSketchWithHeap);
        assert!(!p.mergeable && !p.subtractable && p.subpopulation_aware);
    }

    #[test]
    fn sum_mergeable_subtractable_not_subpop() {
        let p = sketch_properties(AggregationType::Sum);
        assert!(p.mergeable && p.subtractable && !p.subpopulation_aware);
    }

    #[test]
    fn kll_mergeable_not_subtractable() {
        let p = sketch_properties(AggregationType::DatasketchesKLL);
        assert!(p.mergeable && !p.subtractable);
    }
}
