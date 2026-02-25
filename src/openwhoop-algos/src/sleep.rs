use chrono::{NaiveDate, NaiveDateTime, TimeDelta};
use whoop::ParsedHistoryReading;

use super::ActivityPeriod;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SleepCycle {
    pub id: NaiveDate,
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
    pub min_bpm: u8,
    pub max_bpm: u8,
    pub avg_bpm: u8,
    pub min_hrv: u16,
    pub max_hrv: u16,
    pub avg_hrv: u16,
    /// Average respiratory rate in breaths per minute, derived from
    /// RSA (Respiratory Sinus Arrhythmia) zero-crossing of the RR series.
    /// 0.0 if insufficient RR data was available.
    pub avg_resp_rate: f64,
    pub score: f64,
}

impl SleepCycle {
    pub fn from_event(event: ActivityPeriod, history: &[ParsedHistoryReading]) -> SleepCycle {
        let (heart_rate, rr): (Vec<u64>, Vec<Vec<_>>) = history
            .iter()
            .filter(|h| h.time >= event.start && h.time <= event.end)
            .map(|h| (h.bpm as u64, h.rr.clone()))
            .unzip();

        let rr = Self::clean_rr(rr);
        let rolling_hrv = Self::rolling_hrv(rr.clone());

        let min_hrv = rolling_hrv.iter().min().copied().unwrap_or_default() as u16;
        let max_hrv = rolling_hrv.iter().max().copied().unwrap_or_default() as u16;

        let hrv_count = rolling_hrv.len() as u64;
        let avg_hrv = if hrv_count == 0 {
            0u16
        } else {
            (rolling_hrv.into_iter().sum::<u64>() / hrv_count) as u16
        };

        let min_bpm = heart_rate.iter().min().copied().unwrap_or_default() as u8;
        let max_bpm = heart_rate.iter().max().copied().unwrap_or_default() as u8;

        let heart_rate_count = heart_rate.len() as u64;
        let avg_bpm = if heart_rate_count == 0 {
            0u8
        } else {
            (heart_rate.into_iter().sum::<u64>() / heart_rate_count) as u8
        };

        let avg_resp_rate = Self::estimate_respiratory_rate(&rr);

        let id = event.end.date();

        Self {
            id,
            start: event.start,
            end: event.end,
            min_bpm,
            max_bpm,
            avg_bpm,
            min_hrv,
            max_hrv,
            avg_hrv,
            avg_resp_rate,
            score: Self::sleep_score(event.start, event.end),
        }
    }

    pub fn duration(&self) -> TimeDelta {
        self.end - self.start
    }

    fn clean_rr(rr: Vec<Vec<u16>>) -> Vec<u64> {
        rr.into_iter()
            .filter_map(|rr| {
                if rr.is_empty() {
                    return None;
                }
                let count = rr.len() as u64;
                let rr_sum = rr.into_iter().map(u64::from).sum::<u64>();
                Some(rr_sum / count)
            })
            .collect()
    }

    fn rolling_hrv(rr: Vec<u64>) -> Vec<u64> {
        rr.windows(300).filter_map(Self::calculate_rmssd).collect()
    }

    fn calculate_rmssd(window: &[u64]) -> Option<u64> {
        if window.len() < 2 {
            return None;
        }

        let rr_diff: Vec<f64> = window
            .windows(2)
            .map(|w| (w[1] as f64 - w[0] as f64).powi(2))
            .collect();

        let rr_count = rr_diff.len() as f64;
        Some((rr_diff.into_iter().sum::<f64>() / rr_count).sqrt() as u64)
    }

    /// Estimate respiratory rate (breaths/min) from a sequence of RR intervals
    /// using the Respiratory Sinus Arrhythmia (RSA) technique.
    ///
    /// Breathing causes a cyclical oscillation in RR intervals: HR rises on
    /// inhalation, falls on exhalation. We isolate that oscillation by
    /// subtracting a slow moving average from the raw RR series, then counting
    /// zero-crossings of the residual. Each up→down crossing is one breath.
    ///
    /// Works best during sleep when the oscillation is cleanest. Requires at
    /// least ~150 RR values to get a meaningful 60-second window.
    fn estimate_respiratory_rate(rr: &[u64]) -> f64 {
        // Need at least a couple of minutes of data
        const MIN_RR_COUNT: usize = 120;
        if rr.len() < MIN_RR_COUNT {
            return 0.0;
        }

        // Process the full series in 60-second windows and average the results.
        // A resting heart rate of ~60 bpm gives ~60 RR values per minute, but
        // during sleep it's typically 50-70 values/minute. We use a fixed window
        // of 60 samples which approximates ~60 seconds.
        const WINDOW_SECS: usize = 60;   // samples ≈ seconds at resting HR
        const SMOOTH_WIDTH: usize = 15;   // moving average width for baseline

        let mut rates: Vec<f64> = Vec::new();

        for window in rr.windows(WINDOW_SECS) {
            // 1. Compute moving-average baseline (removes slow drift)
            let half = SMOOTH_WIDTH / 2;
            let baseline: Vec<f64> = (0..window.len())
                .map(|i| {
                    let lo = i.saturating_sub(half);
                    let hi = (i + half + 1).min(window.len());
                    let slice = &window[lo..hi];
                    slice.iter().sum::<u64>() as f64 / slice.len() as f64
                })
                .collect();

            // 2. Residual = raw - baseline isolates the respiratory oscillation
            let residual: Vec<f64> = window
                .iter()
                .zip(baseline.iter())
                .map(|(&r, &b)| r as f64 - b)
                .collect();

            // 3. Count positive→negative zero crossings (each = one exhalation)
            let crossings = residual
                .windows(2)
                .filter(|w| w[0] >= 0.0 && w[1] < 0.0)
                .count();

            if crossings > 0 {
                // Duration of this window in seconds (sum of RR intervals / 1000)
                let window_duration_s = window.iter().sum::<u64>() as f64 / 1000.0;
                let rate = (crossings as f64 / window_duration_s) * 60.0;
                // Physiologically valid respiratory rate: 6-30 breaths/min
                if (6.0..=30.0).contains(&rate) {
                    rates.push(rate);
                }
            }
        }

        if rates.is_empty() {
            return 0.0;
        }

        // Return median to reduce impact of outlier windows
        rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = rates.len() / 2;
        if rates.len() % 2 == 0 {
            ((rates[mid - 1] + rates[mid]) / 2.0 * 10.0).round() / 10.0
        } else {
            (rates[mid] * 10.0).round() / 10.0
        }
    }

    pub fn sleep_score(start: NaiveDateTime, end: NaiveDateTime) -> f64 {
        let duration = (end - start).num_seconds() as f64;
        const IDEAL_DURATION: f64 = (60 * 60 * 8) as f64;

        let score = duration / IDEAL_DURATION;

        (score * 100.0).clamp(0.0, 100.0)
    }
}
