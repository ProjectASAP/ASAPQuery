/// Manages tumbling and sliding window boundaries and detects which windows
/// have closed based on watermark advancement.
///
/// Tumbling windows are a special case where `slide_interval == window_size`.
/// The same logic handles both — no separate code paths.
pub struct WindowManager {
    /// Window size in milliseconds.
    window_size_ms: i64,
    /// Slide interval in milliseconds (== window_size_ms for tumbling windows).
    slide_interval_ms: i64,
}

impl WindowManager {
    /// Create a new WindowManager.
    ///
    /// `window_size_secs` and `slide_interval_secs` come from `AggregationConfig`
    /// (which stores them in seconds). They are converted to milliseconds internally.
    pub fn new(window_size_secs: u64, slide_interval_secs: u64) -> Self {
        let window_size_ms = (window_size_secs * 1000) as i64;
        let slide_interval_ms = if slide_interval_secs == 0 {
            window_size_ms // tumbling window
        } else {
            (slide_interval_secs * 1000) as i64
        };
        Self {
            window_size_ms,
            slide_interval_ms,
        }
    }

    pub fn window_size_ms(&self) -> i64 {
        self.window_size_ms
    }

    /// Compute the window start for a given timestamp.
    /// Windows are aligned to epoch (multiples of slide_interval_ms).
    pub fn window_start_for(&self, timestamp_ms: i64) -> i64 {
        // Floor-divide to the nearest slide interval boundary
        let n = timestamp_ms.div_euclid(self.slide_interval_ms);
        n * self.slide_interval_ms
    }

    /// Return window starts whose windows are now closed, given that the
    /// watermark advanced from `previous_wm` to `current_wm`.
    ///
    /// A window `[start, start + window_size_ms)` is closed when
    /// `current_wm >= start + window_size_ms`.
    ///
    /// Returns window starts in ascending order.
    pub fn closed_windows(&self, previous_wm: i64, current_wm: i64) -> Vec<i64> {
        if current_wm <= previous_wm || previous_wm == i64::MIN {
            // No watermark advancement, or first sample ever (nothing to close yet
            // — the window that contains the first sample is still open).
            return Vec::new();
        }

        let mut closed = Vec::new();

        // The earliest window start that *could* have been open at previous_wm.
        // A window is open if its end (start + window_size_ms) > previous_wm.
        // So the oldest open window start was: previous_wm - window_size_ms + 1,
        // aligned down to slide_interval.
        let earliest_open_start =
            self.window_start_for((previous_wm - self.window_size_ms + 1).max(0));

        let mut start = earliest_open_start;
        while start + self.window_size_ms <= current_wm {
            // This window was NOT closed at previous_wm but IS closed at current_wm
            if start + self.window_size_ms > previous_wm {
                closed.push(start);
            }
            start += self.slide_interval_ms;
        }

        closed
    }

    /// Return all window starts whose window `[start, start + window_size_ms)`
    /// contains the given timestamp. For tumbling windows this returns exactly
    /// one start; for sliding windows it returns `ceil(window_size / slide)`
    /// starts.
    pub fn window_starts_containing(&self, timestamp_ms: i64) -> Vec<i64> {
        let mut starts = Vec::new();
        let mut start = self.window_start_for(timestamp_ms);
        while start + self.window_size_ms > timestamp_ms {
            starts.push(start);
            start -= self.slide_interval_ms;
        }
        starts
    }

    /// Return the window `[start, end)` boundaries for a given window start.
    pub fn window_bounds(&self, window_start: i64) -> (i64, i64) {
        (window_start, window_start + self.window_size_ms)
    }

    /// Slide interval accessor.
    pub fn slide_interval_ms(&self) -> i64 {
        self.slide_interval_ms
    }

    /// Pane start for a timestamp. Panes are aligned to the slide_interval grid,
    /// which is the same grid as `window_start_for`.
    pub fn pane_start_for(&self, timestamp_ms: i64) -> i64 {
        self.window_start_for(timestamp_ms)
    }

    /// All pane starts composing a window, in ascending order.
    /// A window `[ws, ws + window_size)` is composed of
    /// `window_size / slide_interval` consecutive panes.
    pub fn panes_for_window(&self, window_start: i64) -> Vec<i64> {
        let num_panes = self.window_size_ms / self.slide_interval_ms;
        (0..num_panes)
            .map(|i| window_start + i * self.slide_interval_ms)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_window_start() {
        // 60-second (60000ms) tumbling windows
        let wm = WindowManager::new(60, 0);

        assert_eq!(wm.window_start_for(0), 0);
        assert_eq!(wm.window_start_for(59_999), 0);
        assert_eq!(wm.window_start_for(60_000), 60_000);
        assert_eq!(wm.window_start_for(119_999), 60_000);
        assert_eq!(wm.window_start_for(120_000), 120_000);
    }

    #[test]
    fn test_no_closed_windows_on_first_sample() {
        let wm = WindowManager::new(60, 0);
        let closed = wm.closed_windows(i64::MIN, 30_000);
        assert!(closed.is_empty());
    }

    #[test]
    fn test_tumbling_window_close() {
        // 60s tumbling windows
        let wm = WindowManager::new(60, 0);

        // Watermark advances from 30_000 to 70_000
        // Window [0, 60_000) closes when wm >= 60_000
        let closed = wm.closed_windows(30_000, 70_000);
        assert_eq!(closed, vec![0]);
    }

    #[test]
    fn test_multiple_window_closes() {
        // 10s (10000ms) tumbling windows
        let wm = WindowManager::new(10, 0);

        // Watermark jumps from 5_000 to 35_000 — closes windows 0, 10_000, 20_000
        let closed = wm.closed_windows(5_000, 35_000);
        assert_eq!(closed, vec![0, 10_000, 20_000]);
    }

    #[test]
    fn test_no_close_when_watermark_stagnant() {
        let wm = WindowManager::new(60, 0);
        let closed = wm.closed_windows(30_000, 30_000);
        assert!(closed.is_empty());
    }

    #[test]
    fn test_window_bounds() {
        let wm = WindowManager::new(60, 0);
        assert_eq!(wm.window_bounds(0), (0, 60_000));
        assert_eq!(wm.window_bounds(60_000), (60_000, 120_000));
    }

    #[test]
    fn test_sliding_window() {
        // 30s window, 10s slide
        let wm = WindowManager::new(30, 10);

        assert_eq!(wm.window_start_for(0), 0);
        assert_eq!(wm.window_start_for(9_999), 0);
        assert_eq!(wm.window_start_for(10_000), 10_000);

        // Watermark advances from 15_000 to 35_000
        // Window [0, 30_000) closes at wm=30_000 (was open at 15_000)
        let closed = wm.closed_windows(15_000, 35_000);
        assert_eq!(closed, vec![0]);
    }

    #[test]
    fn test_window_starts_containing_tumbling() {
        // 60s tumbling windows — each sample belongs to exactly one window
        let wm = WindowManager::new(60, 0);
        let mut starts = wm.window_starts_containing(15_000);
        starts.sort();
        assert_eq!(starts, vec![0]);

        let mut starts = wm.window_starts_containing(60_000);
        starts.sort();
        assert_eq!(starts, vec![60_000]);
    }

    #[test]
    fn test_window_starts_containing_sliding() {
        // 30s window, 10s slide — each sample belongs to 3 windows
        let wm = WindowManager::new(30, 10);

        // t=15_000 belongs to [0, 30_000), [10_000, 40_000)
        // and [-10_000, 20_000) which starts negative — still returned
        let mut starts = wm.window_starts_containing(15_000);
        starts.sort();
        assert_eq!(starts, vec![-10_000, 0, 10_000]);

        // t=30_000 belongs to [10_000, 40_000), [20_000, 50_000), [30_000, 60_000)
        let mut starts = wm.window_starts_containing(30_000);
        starts.sort();
        assert_eq!(starts, vec![10_000, 20_000, 30_000]);
    }

    // --- Pane method tests ---

    #[test]
    fn test_pane_start_for_equals_window_start_for() {
        // Pane start and window start use the same slide-aligned grid
        let wm = WindowManager::new(30, 10);
        for ts in [0, 5_000, 9_999, 10_000, 15_000, 25_000, 30_000] {
            assert_eq!(wm.pane_start_for(ts), wm.window_start_for(ts));
        }
    }

    #[test]
    fn test_panes_for_window_sliding() {
        // 30s window, 10s slide → 3 panes per window
        let wm = WindowManager::new(30, 10);

        assert_eq!(wm.panes_for_window(0), vec![0, 10_000, 20_000]);
        assert_eq!(wm.panes_for_window(10_000), vec![10_000, 20_000, 30_000]);
        assert_eq!(wm.panes_for_window(20_000), vec![20_000, 30_000, 40_000]);
    }

    #[test]
    fn test_panes_for_window_tumbling_degeneration() {
        // 60s tumbling window → 1 pane per window (no merges needed)
        let wm = WindowManager::new(60, 0);

        assert_eq!(wm.panes_for_window(0), vec![0]);
        assert_eq!(wm.panes_for_window(60_000), vec![60_000]);
    }

    #[test]
    fn test_slide_interval_ms_accessor() {
        let wm_tumbling = WindowManager::new(60, 0);
        assert_eq!(wm_tumbling.slide_interval_ms(), 60_000);

        let wm_sliding = WindowManager::new(30, 10);
        assert_eq!(wm_sliding.slide_interval_ms(), 10_000);
    }

    #[test]
    fn test_panes_for_window_count() {
        // W = window_size / slide_interval
        let wm = WindowManager::new(30, 10);
        assert_eq!(wm.panes_for_window(0).len(), 3); // 30/10 = 3

        let wm2 = WindowManager::new(50, 10);
        assert_eq!(wm2.panes_for_window(0).len(), 5); // 50/10 = 5

        let wm3 = WindowManager::new(60, 0);
        assert_eq!(wm3.panes_for_window(0).len(), 1); // tumbling = 1
    }

    #[test]
    fn test_consecutive_windows_share_panes() {
        // 30s window, 10s slide — consecutive windows share W-1 = 2 panes
        let wm = WindowManager::new(30, 10);

        let panes_a = wm.panes_for_window(0); // [0, 10_000, 20_000]
        let panes_b = wm.panes_for_window(10_000); // [10_000, 20_000, 30_000]

        // Shared panes: 10_000 and 20_000
        let shared: Vec<i64> = panes_a
            .iter()
            .filter(|p| panes_b.contains(p))
            .copied()
            .collect();
        assert_eq!(shared, vec![10_000, 20_000]);
    }
}
