use crate::stores::TimestampRange;
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SlidingWindowSpec {
    pub(crate) window_size_ms: u64,
    pub(crate) slide_interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExactWindowCover {
    pub(crate) aligned_end_ms: u64,
    pub(crate) windows: Vec<TimestampRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompositionError {
    ZeroWindowSize,
    ZeroSlideInterval,
    ZeroLookback,
    WindowNotMultipleOfSlide {
        window_size_ms: u64,
        slide_interval_ms: u64,
    },
    LookbackBeforeEpoch {
        aligned_end_ms: u64,
        lookback_ms: u64,
    },
    WindowAllocationFailed {
        window_count: u64,
    },
    LookbackNotMultiple {
        lookback_ms: u64,
        window_size_ms: u64,
    },
}

pub(crate) fn plan_exact_cover(
    query_end_ms: u64,
    lookback_ms: u64,
    spec: SlidingWindowSpec,
) -> Result<ExactWindowCover, CompositionError> {
    if spec.window_size_ms == 0 {
        return Err(CompositionError::ZeroWindowSize);
    }
    if spec.slide_interval_ms == 0 {
        return Err(CompositionError::ZeroSlideInterval);
    }
    if lookback_ms == 0 {
        return Err(CompositionError::ZeroLookback);
    }
    if !spec.window_size_ms.is_multiple_of(spec.slide_interval_ms) {
        return Err(CompositionError::WindowNotMultipleOfSlide {
            window_size_ms: spec.window_size_ms,
            slide_interval_ms: spec.slide_interval_ms,
        });
    }
    if !lookback_ms.is_multiple_of(spec.window_size_ms) {
        return Err(CompositionError::LookbackNotMultiple {
            lookback_ms,
            window_size_ms: spec.window_size_ms,
        });
    }

    let aligned_end_ms = query_end_ms - (query_end_ms % spec.slide_interval_ms);
    if aligned_end_ms != query_end_ms {
        warn!(
            query_end_ms,
            aligned_end_ms,
            slide_interval_ms = spec.slide_interval_ms,
            "Sliding query end was aligned down; the requested timestamp will not be used as-is"
        );
    }
    let start_ms =
        aligned_end_ms
            .checked_sub(lookback_ms)
            .ok_or(CompositionError::LookbackBeforeEpoch {
                aligned_end_ms,
                lookback_ms,
            })?;
    let window_count = lookback_ms / spec.window_size_ms;
    let capacity = usize::try_from(window_count)
        .map_err(|_| CompositionError::WindowAllocationFailed { window_count })?;
    let mut windows = Vec::new();
    windows
        .try_reserve_exact(capacity)
        .map_err(|_| CompositionError::WindowAllocationFailed { window_count })?;
    for index in 0..window_count {
        let start = start_ms + index * spec.window_size_ms;
        windows.push((start, start + spec.window_size_ms));
    }

    Ok(ExactWindowCover {
        aligned_end_ms,
        windows,
    })
}

#[cfg(test)]
mod tests {
    use super::{plan_exact_cover, CompositionError, SlidingWindowSpec};

    #[test]
    fn wider_lookback_is_covered_by_non_overlapping_stored_windows() {
        let cover = plan_exact_cover(
            10_000,
            10_000,
            SlidingWindowSpec {
                window_size_ms: 5_000,
                slide_interval_ms: 1_000,
            },
        )
        .expect("the lookback is exactly composable");

        assert_eq!(cover.aligned_end_ms, 10_000);
        assert_eq!(cover.windows, vec![(0, 5_000), (5_000, 10_000)]);
    }

    #[test]
    fn rejects_lookback_that_is_not_a_multiple_of_the_stored_window() {
        let error = plan_exact_cover(
            12_000,
            12_000,
            SlidingWindowSpec {
                window_size_ms: 5_000,
                slide_interval_ms: 1_000,
            },
        )
        .expect_err("partial stored windows cannot form an exact cover");

        assert_eq!(
            error,
            CompositionError::LookbackNotMultiple {
                lookback_ms: 12_000,
                window_size_ms: 5_000,
            }
        );
    }

    #[test]
    fn rejects_zero_sized_window_without_panicking() {
        let error = plan_exact_cover(
            10_000,
            10_000,
            SlidingWindowSpec {
                window_size_ms: 0,
                slide_interval_ms: 1_000,
            },
        )
        .expect_err("a zero-sized stored window is invalid");

        assert_eq!(error, CompositionError::ZeroWindowSize);
    }

    #[test]
    fn rejects_zero_slide_without_panicking() {
        let error = plan_exact_cover(
            10_000,
            10_000,
            SlidingWindowSpec {
                window_size_ms: 5_000,
                slide_interval_ms: 0,
            },
        )
        .expect_err("a zero slide is invalid");

        assert_eq!(error, CompositionError::ZeroSlideInterval);
    }

    #[test]
    fn rejects_zero_lookback() {
        let error = plan_exact_cover(
            10_000,
            0,
            SlidingWindowSpec {
                window_size_ms: 5_000,
                slide_interval_ms: 1_000,
            },
        )
        .expect_err("a query must cover a positive interval");

        assert_eq!(error, CompositionError::ZeroLookback);
    }

    #[test]
    fn rejects_window_width_that_does_not_land_on_the_slide_grid() {
        let error = plan_exact_cover(
            12_000,
            12_000,
            SlidingWindowSpec {
                window_size_ms: 6_000,
                slide_interval_ms: 4_000,
            },
        )
        .expect_err("W-spaced cover boundaries must exist on the S grid");

        assert_eq!(
            error,
            CompositionError::WindowNotMultipleOfSlide {
                window_size_ms: 6_000,
                slide_interval_ms: 4_000,
            }
        );
    }

    #[test]
    fn rejects_lookback_before_the_unix_epoch_without_panicking() {
        let error = plan_exact_cover(
            4_500,
            10_000,
            SlidingWindowSpec {
                window_size_ms: 5_000,
                slide_interval_ms: 1_000,
            },
        )
        .expect_err("the aligned query interval starts before epoch zero");

        assert_eq!(
            error,
            CompositionError::LookbackBeforeEpoch {
                aligned_end_ms: 4_000,
                lookback_ms: 10_000,
            }
        );
    }

    #[test]
    fn rejects_an_exact_cover_too_large_to_allocate() {
        let error = plan_exact_cover(
            u64::MAX,
            u64::MAX,
            SlidingWindowSpec {
                window_size_ms: 1,
                slide_interval_ms: 1,
            },
        )
        .expect_err("allocation failure must be recoverable");

        assert_eq!(
            error,
            CompositionError::WindowAllocationFailed {
                window_count: u64::MAX,
            }
        );
    }
}
