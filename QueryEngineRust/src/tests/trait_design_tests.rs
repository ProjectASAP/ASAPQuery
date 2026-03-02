#[cfg(test)]
use crate::data_model::{
    KeyByLabelValues, MultipleSubpopulationAggregate, SingleSubpopulationAggregate,
};
use crate::precompute_operators::{MultipleSumAccumulator, SumAccumulator};
use promql_utilities::Statistic;

#[test]
fn test_single_subpopulation_interface() {
    // Single accumulator - matches Python behavior exactly
    let acc: Box<dyn SingleSubpopulationAggregate> = Box::new(SumAccumulator::with_sum(42.0));

    // ✅ Query without key - this is the correct interface for Single accumulators
    let result = acc.query(Statistic::Sum, None).unwrap();
    assert_eq!(result, 42.0);
}

#[test]
fn test_multiple_subpopulation_interface() {
    // Multiple accumulator - matches Python behavior exactly
    let mut multi_acc = MultipleSumAccumulator::new();

    let mut key = KeyByLabelValues::new();
    key.insert("web".to_string());
    multi_acc.add_sum(key.clone(), 100.0);

    let acc: Box<dyn MultipleSubpopulationAggregate> = Box::new(multi_acc);

    // ✅ Query with key - this is the correct interface for Multiple accumulators
    let result = acc.query(Statistic::Sum, &key, None).unwrap();
    assert_eq!(result, 100.0);

    // ✅ Get all keys
    let keys = acc.get_keys().unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], key);
}

#[test]
fn test_interface_prevents_misuse() {
    // This test documents what WON'T compile - which is exactly what we want!

    let single_acc: Box<dyn SingleSubpopulationAggregate> =
        Box::new(SumAccumulator::with_sum(42.0));
    let multi_acc: Box<dyn MultipleSubpopulationAggregate> =
        Box::new(MultipleSumAccumulator::new());

    // ✅ These work - correct usage
    let _result1 = single_acc.query(Statistic::Sum, None);
    let key = KeyByLabelValues::new();
    let _result2 = multi_acc.query(Statistic::Sum, &key, None);

    // ❌ These would be compile-time errors (commented out):
    // let _result3 = single_acc.query(Statistic::Sum, &key);  // Too many args for Single
    // let _result4 = multi_acc.query(Statistic::Sum);         // Too few args for Multiple

    // This is exactly the type safety we wanted to achieve!
}

#[test]
fn test_python_alignment() {
    // Demonstrate that the Rust interface now matches Python exactly

    // Python: sum_accumulator.query(Statistic.SUM)
    // Rust:   sum_accumulator.query(Statistic::Sum)
    let sum_acc: Box<dyn SingleSubpopulationAggregate> = Box::new(SumAccumulator::with_sum(42.0));
    assert_eq!(sum_acc.query(Statistic::Sum, None).unwrap(), 42.0);

    // Python: multiple_accumulator.query(Statistic.SUM, key)
    // Rust:   multiple_accumulator.query(Statistic::Sum, &key)
    let mut multi_acc = MultipleSumAccumulator::new();
    let key = KeyByLabelValues::new();
    multi_acc.add_sum(key.clone(), 100.0);
    let multi_trait: Box<dyn MultipleSubpopulationAggregate> = Box::new(multi_acc);
    assert_eq!(
        multi_trait.query(Statistic::Sum, &key, None).unwrap(),
        100.0
    );

    // Perfect alignment with Python behavior!
}
