//! Integration test that runs the library test suite with the sketchlib backend.
//!
//! When you run `cargo test -p query_engine_rust` (without --features sketchlib-tests),
//! the lib tests run with the legacy backend. This test spawns a second run with the
//! sketchlib backend so both modes are exercised in one `cargo test` invocation.
//!
//! This test is only compiled when sketchlib-tests is NOT enabled, to avoid recursion.

#[cfg(not(feature = "sketchlib-tests"))]
#[test]
fn test_sketchlib_backend() {
    use std::process::Command;

    let status = Command::new(env!("CARGO"))
        .args([
            "test",
            "-p",
            "query_engine_rust",
            "--lib",
            "--features",
            "sketchlib-tests",
        ])
        .status()
        .expect("failed to spawn cargo test");

    assert!(
        status.success(),
        "sketchlib backend tests failed (run `cargo test -p query_engine_rust --lib --features sketchlib-tests` for details)"
    );
}
