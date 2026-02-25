#[macro_use]
extern crate log;

use std::{
    io,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::anyhow;
use btleplug::{
    api::{BDAddr, Central, Manager as _, Peripheral as _, ScanFilter},
    platform::{Adapter, Manager, Peripheral},
};
use chrono::{DateTime, Local, NaiveDateTime, NaiveTime, TimeDelta, Utc};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{Shell, generate};
use db_entities::packets;
use dotenv::dotenv;
use openwhoop::{
    OpenWhoop, WhoopDevice,
    algo::{ExerciseMetrics, SleepConsistencyAnalyzer},
    db::DatabaseHandler,
    types::activities::{ActivityType, SearchActivityPeriods},
};
use tokio::time::sleep;
use whoop::{WhoopPacket, constants::WHOOP_SERVICE};

#[cfg(target_os = "linux")]
pub type DeviceId = BDAddr;

#[cfg(target_os = "macos")]
pub type DeviceId = String;

#[derive(Parser)]
pub struct OpenWhoopCli {
    #[arg(env, long)]
    pub debug_packets: bool,
    #[arg(env, long, default_value = "sqlite://openwhoop.db")]
    pub database_url: String,
    #[cfg(target_os = "linux")]
    #[arg(env, long)]
    pub ble_interface: Option<String>,
    #[clap(subcommand)]
    pub subcommand: OpenWhoopCommand,
}

#[derive(Subcommand)]
pub enum OpenWhoopCommand {
    ///
    /// Scan for Whoop devices
    ///
    Scan,
    ///
    /// Process all user data (events, stats, stress) and save to Firestore
    ///
    ProcessUser { user_id: String },
    ///
    /// Set alarm
    ///
    SetAlarm {
        #[arg(long, env)]
        whoop: DeviceId,
        alarm_time: AlarmTime,
    },
    ///
    /// Copy packets from one database into another
    ///
    Merge { from: String },
    Restart {
        #[arg(long, env)]
        whoop: DeviceId,
    },
    ///
    /// Get device firmware version info
    ///
    Version {
        #[arg(long, env)]
        whoop: DeviceId,
    },
    ///
    /// Generate Shell completions
    ///
    Completions { shell: Shell },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Err(error) = dotenv() {
        println!("{}", error);
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .filter_module("sqlx::query", log::LevelFilter::Off)
        .filter_module("sea_orm_migration::migrator", log::LevelFilter::Off)
        .filter_module("bluez_async", log::LevelFilter::Off)
        .init();

    let cli = OpenWhoopCli::parse();

    match cli.subcommand {
        OpenWhoopCommand::ProcessUser { user_id } => {
            let lookback_hours = std::env::var("LOOKBACK_HOURS")
                .unwrap_or_else(|_| "168".to_string())
                .parse::<u32>()
                .unwrap_or(168);

            info!("Processing data for user {} with lookback of {} hours", user_id, lookback_hours);
            let mut all_readings = fetch_user_history(&user_id, lookback_hours).await?;

            // ── Deduplication ─────────────────────────────────────────────────────
            // BigQuery has no UNIQUE constraint on the timestamp column (unlike the
            // old SQLite schema). Duplicate rows at the same timestamp with different
            // activity labels cause smooth_spikes to erase entire Sleep/Active periods.
            {
                let before = all_readings.len();
                let mut seen = std::collections::HashSet::new();
                all_readings.retain(|r| seen.insert(r.time));
                let dupes = before - all_readings.len();
                if dupes > 0 {
                    warn!(
                        "Removed {} duplicate-timestamp rows (BigQuery lacks UNIQUE \
                         constraint on unix) — this was causing Sleep/Active events to disappear",
                        dupes
                    );
                } else {
                    info!("Dedup: no duplicate timestamps found");
                }
            }

            // ── Check 1: Non-empty dataset ────────────────────────────────────────
            if all_readings.is_empty() {
                error!("No data found for user {} — cannot run detection", user_id);
                return Err(anyhow!("No BigQuery rows returned for user {}", user_id));
            }
            info!("Check 1 PASS: {} total readings fetched", all_readings.len());

            // ── Check 2: Time range sanity ────────────────────────────────────────
            let min_time = all_readings.first().map(|r| r.time).unwrap();
            let max_time = all_readings.last().map(|r| r.time).unwrap();
            let span_hours = (max_time - min_time).num_hours();
            info!("Check 2: Time range {} → {} (~{} hours)", min_time, max_time, span_hours);
            if min_time.and_utc().timestamp() < chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap().timestamp() {
                return Err(anyhow!(
                    "Earliest reading timestamp {} looks like an epoch/default value — \
                     timestamp parsing likely broke during BigQuery fetch. Aborting.",
                    min_time
                ));
            }
            if span_hours < 1 {
                warn!("Time span of {} hours is suspiciously short for a {}-hour lookback window", span_hours, lookback_hours);
            } else {
                info!("Check 2 PASS: time range looks reasonable");
            }

            // ── Check 3: Chronological order ──────────────────────────────────────
            {
                let mut order_ok = true;
                for window in all_readings.windows(2) {
                    if window[1].time < window[0].time {
                        error!(
                            "Check 3 FAIL: readings are not sorted — found {} before {} at indices near the dataset",
                            window[1].time, window[0].time
                        );
                        order_ok = false;
                        break;
                    }
                }
                if !order_ok {
                    return Err(anyhow!(
                        "BigQuery results are not in ascending time order. \
                         The detection algorithm requires sorted data."
                    ));
                }
                info!("Check 3 PASS: readings are in ascending time order");
            }

            // ── Check 4: Data density / gaps ──────────────────────────────────────
            {
                let total_gap_secs: i64 = all_readings
                    .windows(2)
                    .map(|w| (w[1].time - w[0].time).num_seconds())
                    .sum();
                let avg_gap_secs = total_gap_secs / (all_readings.len() as i64 - 1).max(1);
                info!("Check 4: Average gap between readings: {}s", avg_gap_secs);
                if avg_gap_secs > 300 {
                    warn!(
                        "Average gap of {}s (>{} min) is very high — data may be too sparse for accurate detection",
                        avg_gap_secs, avg_gap_secs / 60
                    );
                }
                // Report but do not abort on large individual gaps
                let large_gaps: Vec<_> = all_readings
                    .windows(2)
                    .filter(|w| (w[1].time - w[0].time).num_minutes() > 60)
                    .map(|w| (w[0].time, w[1].time, (w[1].time - w[0].time).num_minutes()))
                    .collect();
                if large_gaps.is_empty() {
                    info!("Check 4 PASS: no gaps > 60 min found");
                } else {
                    warn!("Check 4: found {} gap(s) > 60 min in the data:", large_gaps.len());
                    for (start, end, mins) in &large_gaps {
                        warn!("  Gap: {} → {} ({} min)", start, end, mins);
                    }
                }
            }

            // ── Check 5: BPM sanity ───────────────────────────────────────────────
            {
                let min_bpm = all_readings.iter().map(|r| r.bpm).min().unwrap();
                let max_bpm = all_readings.iter().map(|r| r.bpm).max().unwrap();
                let mean_bpm = all_readings.iter().map(|r| r.bpm as u64).sum::<u64>() / all_readings.len() as u64;
                info!("Check 5: BPM stats — min={} max={} mean={}", min_bpm, max_bpm, mean_bpm);
                if mean_bpm < 20 || mean_bpm > 220 {
                    return Err(anyhow!(
                        "Mean BPM of {} is physiologically implausible — data is likely corrupted or column mapping is wrong",
                        mean_bpm
                    ));
                }
                info!("Check 5 PASS: BPM values look reasonable");
            }

            // ── Check 6: Activity distribution ────────────────────────────────────
            {
                let mut counts = std::collections::HashMap::new();
                for r in &all_readings {
                    *counts.entry(format!("{:?}", r.activity)).or_insert(0u64) += 1;
                }
                info!("Check 6: Activity distribution: {:?}", counts);
                let unknown_count = counts.get("Unknown").copied().unwrap_or(0);
                if unknown_count == all_readings.len() as u64 {
                    return Err(anyhow!(
                        "100% of readings ({}) have Unknown activity — the activity column \
                         mapping is almost certainly broken (check BigQuery column name/type)",
                        unknown_count
                    ));
                } else if unknown_count > 0 {
                    warn!("{} readings ({:.1}%) have Unknown activity",
                        unknown_count,
                        unknown_count as f64 / all_readings.len() as f64 * 100.0
                    );
                } else {
                    info!("Check 6 PASS: no Unknown-activity readings");
                }
            }

            info!("All pre-detection checks passed — running event detection");
            let events = openwhoop_algos::ActivityPeriod::detect(&mut all_readings);
            let mut sleeps = Vec::new();
            let mut exercises = Vec::new();

            for event in events {
                info!("EVENT: {:?} {} → {}", event.activity, event.start, event.end);
                match event.activity {
                    whoop::Activity::Sleep => {
                        if event.duration.num_minutes() < 60 {
                            info!("Discarding sleep cycle shorter than 1 hour ({} mins)", event.duration.num_minutes());
                            continue;
                        }
                        
                        let cycle = openwhoop_algos::SleepCycle::from_event(event, &all_readings);
                        info!(
                            "Sleep cycle HRV: min={} avg={} max={} | resp_rate={:.1} br/min (readings in window: {})",
                            cycle.min_hrv, cycle.avg_hrv, cycle.max_hrv,
                            cycle.avg_resp_rate,
                            all_readings.iter().filter(|r| r.time >= event.start && r.time <= event.end).count()
                        );
                        sleeps.push(cycle);
                    }
                    whoop::Activity::Active => {
                        exercises.push(event);
                    }
                    _ => {}
                }
            }

            // Calculate consistency metrics (optional printing for logs)
            let sleep_analyzer = SleepConsistencyAnalyzer::new(sleeps.clone());
            let sleep_metrics = sleep_analyzer.calculate_consistency_metrics();
            println!("Sleep Metrics:\n{}", sleep_metrics);

            let exercise_metrics = ExerciseMetrics::new(exercises.clone());
            println!("Exercise Metrics:\n{}", exercise_metrics);

            // Extract the most recent sleep's avg HRV as the personal baseline.
            // The stress calculator uses this to produce a ratio-based score
            // (waking RMSSD relative to sleep recovery HRV) rather than an
            // absolute value that varies widely between individuals.
            let sleep_baseline_hrv: Option<f64> = sleeps
                .last()
                .map(|s| s.avg_hrv as f64)
                .filter(|&v| v > 0.0);

            let stress_score = openwhoop_algos::StressCalculator::calculate_stress(
                &all_readings,
                sleep_baseline_hrv,
            );
            if let Some(stress) = &stress_score {
                info!(
                    "Stress score: {:.2} | waking RMSSD: {:.1} ms | baseline HRV: {:?}",
                    stress.score, stress.waking_rmssd, stress.baseline_hrv
                );
            } else {
                info!("Not enough recent waking data to calculate stress score");
            }

            println!("Saving User Data to Firestore...");
            let last_processed = all_readings.last().map(|r| r.time).unwrap_or_default();
            write_firestore_events(&user_id, &all_readings, &sleeps, &exercises, stress_score, last_processed).await?;
            println!("Successfully processed user {}", user_id);

            return Ok(());
        }
        OpenWhoopCommand::Completions { shell } => {
            let mut command = OpenWhoopCli::command();
            let bin_name = command.get_name().to_string();
            clap_complete::generate(shell, &mut command, bin_name, &mut std::io::stdout());
            return Ok(());
        }
        _ => {}
    }

    let adapter = cli.create_ble_adapter().await?;
    let db_handler = DatabaseHandler::new(cli.database_url).await;

    match cli.subcommand {
        OpenWhoopCommand::Scan => {
            scan_command(&adapter, None).await?;
            Ok(())
        }


        OpenWhoopCommand::SetAlarm { whoop, alarm_time } => {
            let peripheral = scan_command(&adapter, Some(whoop)).await?;
            let mut whoop = WhoopDevice::new(peripheral, adapter, db_handler, cli.debug_packets);
            whoop.connect().await?;

            let time = alarm_time.unix();
            let now = Utc::now();

            if time < now {
                error!(
                    "Time {} is in past, current time: {}",
                    time.format("%Y-%m-%d %H:%M:%S"),
                    now.format("%Y-%m-%d %H:%M:%S")
                );
                return Ok(());
            }

            let packet = WhoopPacket::alarm_time(time.timestamp() as u32);
            whoop.send_command(packet).await?;
            let time = time.with_timezone(&Local);

            println!("Alarm time set for: {}", time.format("%Y-%m-%d %H:%M:%S"));
            Ok(())
        }
        OpenWhoopCommand::Merge { from } => {
            let from_db = DatabaseHandler::new(from).await;

            let mut id = 0;
            loop {
                let packets = from_db.get_packets(id).await?;
                if packets.is_empty() {
                    break;
                }

                for packets::Model {
                    uuid,
                    bytes,
                    id: c_id,
                } in packets
                {
                    id = c_id;
                    db_handler.create_packet(uuid, bytes).await?;
                }

                println!("{}", id);
            }

            Ok(())
        }
        OpenWhoopCommand::Restart { whoop } => {
            let peripheral = scan_command(&adapter, Some(whoop)).await?;
            let mut whoop = WhoopDevice::new(peripheral, adapter, db_handler, cli.debug_packets);
            whoop.connect().await?;
            whoop.send_command(WhoopPacket::restart()).await?;
            Ok(())
        }
        OpenWhoopCommand::Version { whoop } => {
            let peripheral = scan_command(&adapter, Some(whoop)).await?;
            let mut whoop = WhoopDevice::new(peripheral, adapter, db_handler, false);
            whoop.connect().await?;
            whoop.get_version().await?;
            Ok(())
        }
        _ => Ok(()), // Handled earlier
    }
}

async fn fetch_user_history(user_id: &str, hours: u32) -> anyhow::Result<Vec<whoop::ParsedHistoryReading>> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap_or_else(|_| "your-project-id".to_string());
    let dataset_id = std::env::var("BIGQUERY_DATASET").unwrap_or_else(|_| "whoop_data".to_string());

    let bq_client = gcp_bigquery_client::Client::from_application_default_credentials()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to init BigQuery client: {}", e))?;

    let query = format!(
        "SELECT unix, bpm, rr, activity 
         FROM `{project_id}.{dataset_id}.heart_rate` 
         WHERE uid = '{user_id}' AND unix >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
         ORDER BY unix ASC, activity DESC"
    );

    let mut query_request = gcp_bigquery_client::model::query_request::QueryRequest::new(query);
    query_request.use_legacy_sql = false;

    let mut response = bq_client
        .job()
        .query(&project_id, query_request)
        .await
        .map_err(|e| anyhow::anyhow!("BigQuery query failed: {}", e))?;

    let mut readings = Vec::new();
    let mut rows_scanned: u64 = 0;
    let mut rows_skipped_parse: u64 = 0;
    let mut rows_skipped_bpm: u64 = 0;
    let mut rows_skipped_epoch: u64 = 0;
    let epoch_cutoff = chrono::NaiveDateTime::parse_from_str("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

    loop {
        let mut rs = gcp_bigquery_client::model::query_response::ResultSet::new_from_query_response(response.clone());

        while rs.next_row() {
            rows_scanned += 1;

            // Unpack all four columns, logging any parse failures with raw values
            let time_result = rs.get_string(0);
            let bpm_result = rs.get_i64(1);
            let rr_result = rs.get_string(2);
            let activity_result = rs.get_i64(3);

            let (time_str, bpm_i, rr_intervals, activity_i) = match (
                time_result,
                bpm_result,
                rr_result,
                activity_result,
            ) {
                (Ok(Some(t)), Ok(Some(b)), Ok(Some(r)), Ok(Some(a))) => (t, b, r, a),
                (t, b, r, a) => {
                    rows_skipped_parse += 1;
                    warn!(
                        "Row {}: failed to parse one or more columns — \
                         unix={:?} bpm={:?} rr={:?} activity={:?}; skipping",
                        rows_scanned, t, b, r, a
                    );
                    continue;
                }
            };

            // Skip bpm=0 rows (band off, no reading) — expected, log at debug only.
            // Reject bpm>250 as a corrupt/impossible value.
            if bpm_i == 0 {
                rows_skipped_bpm += 1;
                debug!("Row {}: BPM=0 (band off), skipping", rows_scanned);
                continue;
            }
            if bpm_i > 250 {
                rows_skipped_bpm += 1;
                warn!(
                    "Row {}: BPM value {} is above physiological maximum (250); skipping",
                    rows_scanned, bpm_i
                );
                continue;
            }

            // Parse timestamp — try RFC 3339 first, then BigQuery's plain UTC format,
            // then as a FLOAT64 unix seconds value (BigQuery returns these as scientific
            // notation strings like "1.77169068E9").
            let time = match chrono::DateTime::parse_from_rfc3339(&time_str) {
                Ok(t) => t.naive_utc(),
                Err(_) => match chrono::NaiveDateTime::parse_from_str(&time_str, "%Y-%m-%d %H:%M:%S UTC") {
                    Ok(t) => t,
                    Err(_) => match time_str.parse::<f64>() {
                        Ok(unix_secs) => {
                            let secs = unix_secs as i64;
                            match chrono::DateTime::from_timestamp(secs, 0) {
                                Some(dt) => dt.naive_utc(),
                                None => {
                                    rows_skipped_parse += 1;
                                    warn!(
                                        "Row {}: unix timestamp {} is out of valid range; skipping",
                                        rows_scanned, secs
                                    );
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            rows_skipped_parse += 1;
                            warn!(
                                "Row {}: could not parse timestamp {:?} — {}: skipping",
                                rows_scanned, time_str, e
                            );
                            continue;
                        }
                    },
                },
            };

            // Reject epoch/default timestamps (indicates parse produced a zero datetime)
            if time < epoch_cutoff {
                rows_skipped_epoch += 1;
                warn!(
                    "Row {}: timestamp {} predates 2020 — likely an epoch/default value from a \
                     failed parse (raw string: {:?}); skipping",
                    rows_scanned, time, time_str
                );
                continue;
            }

            // rr is stored as a JSON array string e.g. "[1440,1538]" or "[]"
            let rr_parsed: Vec<u16> =
                serde_json::from_str::<Vec<u16>>(&rr_intervals).unwrap_or_else(|_| {
                    // Fallback: try plain comma-separated for any legacy rows
                    rr_intervals
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect()
                });

            if !rr_parsed.is_empty() && readings.iter().all(|r: &whoop::ParsedHistoryReading| r.rr.is_empty()) {
                debug!("RR sample (row {}): raw={:?} parsed={:?}", rows_scanned, rr_intervals, rr_parsed);
            }

            let hr = whoop::ParsedHistoryReading {
                time,
                bpm: bpm_i as u8,
                rr: rr_parsed,
                activity: whoop::Activity::from(activity_i),
            };
            readings.push(hr);
        }

        if let Some(token) = response.page_token.clone() {
            if let Some(job_ref) = response.job_reference.as_ref() {
                let job_id = job_ref.job_id.as_ref().unwrap();
                let mut options = gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters::default();
                options.page_token = Some(token);
                options.location = job_ref.location.clone();

                let next_resp = bq_client.job().get_query_results(&project_id, job_id, options).await
                    .map_err(|e| anyhow::anyhow!("BigQuery page fetch failed: {}", e))?;

                response = next_resp.into();
            } else {
                break;
            }
        } else {
            break;
        }
    }

    let readings_with_rr = readings.iter().filter(|r| !r.rr.is_empty()).count();
    info!(
        "BigQuery fetch complete: scanned={} accepted={} skipped_parse={} skipped_bpm={} skipped_epoch={} | \
         readings_with_rr_data={} ({:.1}%)",
        rows_scanned,
        readings.len(),
        rows_skipped_parse,
        rows_skipped_bpm,
        rows_skipped_epoch,
        readings_with_rr,
        if readings.is_empty() { 0.0 } else { readings_with_rr as f64 / readings.len() as f64 * 100.0 },
    );

    if readings_with_rr == 0 {
        warn!("No RR interval data found in any row — HRV will be 0. \
               Check that the 'rr' BigQuery column contains comma-separated u16 millisecond values.");
    }

    if rows_skipped_parse > 0 || rows_skipped_epoch > 0 {
        warn!(
            "{} rows were skipped due to parsing errors — check BigQuery column names/types match \
             the query (unix, bpm, rr, activity)",
            rows_skipped_parse + rows_skipped_epoch
        );
    }

    Ok(readings)
}

async fn write_firestore_events(
    user_id: &str,
    all_readings: &[whoop::ParsedHistoryReading],
    sleeps: &[openwhoop_algos::SleepCycle],
    exercises: &[openwhoop_algos::ActivityPeriod],
    stress_score: Option<openwhoop_algos::StressScore>,
    last_processed: chrono::NaiveDateTime,

) -> anyhow::Result<()> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap_or_else(|_| "your-project-id".to_string());
    let users_col = std::env::var("FIRESTORE_COLLECTION_USERS").unwrap_or_else(|_| "user-data".to_string());

    let firestore_db = firestore::FirestoreDb::new(&project_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to init Firestore client: {}", e))?;

    // Build the new records from this run (with per-minute HR graph data)
    let new_sleep_map: std::collections::HashMap<i64, serde_json::Value> = sleeps
        .iter()
        .map(|sleep| {
            let id = sleep.start.and_utc().timestamp();
            let hr_by_minute = aggregate_hr_by_minute(all_readings, sleep.start, sleep.end);
            (id, serde_json::json!({
                "sleep_id": id,
                "start": id,
                "end": sleep.end.and_utc().timestamp(),
                "min_bpm": sleep.min_bpm,
                "max_bpm": sleep.max_bpm,
                "avg_bpm": sleep.avg_bpm as f64,
                "min_hrv": sleep.min_hrv as f64,
                "max_hrv": sleep.max_hrv as f64,
                "avg_hrv": sleep.avg_hrv as f64,
                "avg_resp_rate": sleep.avg_resp_rate,
                "score": sleep.score,
                "hr_by_minute": hr_by_minute,
            }))
        })
        .collect();

    let new_exercise_map: std::collections::HashMap<i64, serde_json::Value> = exercises
        .iter()
        .map(|exercise| {
            let id = exercise.start.and_utc().timestamp();
            let hr_by_minute = aggregate_hr_by_minute(all_readings, exercise.start, exercise.end);
            (id, serde_json::json!({
                "period_id": id,
                "activity": exercise.activity as u8,
                "start": id,
                "end": exercise.end.and_utc().timestamp(),
                "hr_by_minute": hr_by_minute,
            }))
        })
        .collect();

    // Read the existing Firestore document so we can merge rather than replace.
    // Sleeps and activities are append-only — once recorded they are never removed.
    let existing: Option<serde_json::Value> = firestore_db
        .fluent()
        .select()
        .by_id_in(&users_col)
        .obj::<serde_json::Value>()
        .one(user_id)
        .await
        .unwrap_or(None); // if doc doesn't exist yet, start fresh

    // Merge sleep_cycles: existing identity fields win; hr_by_minute is always
    // refreshed from the current run so graphs improve as data quality improves.
    let mut sleep_map: std::collections::HashMap<i64, serde_json::Value> =
        if let Some(serde_json::Value::Array(arr)) =
            existing.as_ref().and_then(|d| d.get("sleep_cycles"))
        {
            arr.iter()
                .filter_map(|v| v.get("sleep_id")?.as_i64().map(|id| (id, v.clone())))
                .collect()
        } else {
            std::collections::HashMap::new()
        };
    for (id, new_record) in new_sleep_map {
        let new_start = new_record["start"].as_i64().unwrap_or(0);
        let new_end = new_record["end"].as_i64().unwrap_or(0);

        let mut overlapping = Vec::new();
        for (ext_id, ext_record) in &sleep_map {
            let ext_start = ext_record["start"].as_i64().unwrap_or(0);
            let ext_end = ext_record["end"].as_i64().unwrap_or(0);
            
            let overlap_start = new_start.max(ext_start);
            let overlap_end = new_end.min(ext_end);

            if overlap_start < overlap_end && (overlap_end - overlap_start > 1800) {
                overlapping.push(*ext_id);
            }
        }
        for old_id in overlapping {
            sleep_map.remove(&old_id);
        }

        let entry = sleep_map.entry(id).or_insert_with(|| new_record.clone());
        if let Some(hr) = new_record.get("hr_by_minute") {
            entry.as_object_mut().map(|o| o.insert("hr_by_minute".to_string(), hr.clone()));
        }
    }
    let mut merged_sleeps: Vec<serde_json::Value> = sleep_map.into_values().collect();
    merged_sleeps.sort_by_key(|v| v.get("start").and_then(|s| s.as_i64()).unwrap_or(0));
    info!("sleep_cycles after merge: {} total records", merged_sleeps.len());

    let mut exercise_map: std::collections::HashMap<i64, serde_json::Value> =
        if let Some(serde_json::Value::Array(arr)) =
            existing.as_ref().and_then(|d| d.get("activities"))
        {
            arr.iter()
                .filter_map(|v| v.get("period_id")?.as_i64().map(|id| (id, v.clone())))
                .collect()
        } else {
            std::collections::HashMap::new()
        };
    for (id, new_record) in new_exercise_map {
        let new_start = new_record["start"].as_i64().unwrap_or(0);
        let new_end = new_record["end"].as_i64().unwrap_or(0);

        let mut overlapping = Vec::new();
        for (ext_id, ext_record) in &exercise_map {
            let ext_start = ext_record["start"].as_i64().unwrap_or(0);
            let ext_end = ext_record["end"].as_i64().unwrap_or(0);
            
            let overlap_start = new_start.max(ext_start);
            let overlap_end = new_end.min(ext_end);

            if overlap_start < overlap_end && (overlap_end - overlap_start > 900) {
                overlapping.push(*ext_id);
            }
        }
        for old_id in overlapping {
            exercise_map.remove(&old_id);
        }

        let entry = exercise_map.entry(id).or_insert_with(|| new_record.clone());
        if let Some(hr) = new_record.get("hr_by_minute") {
            entry.as_object_mut().map(|o| o.insert("hr_by_minute".to_string(), hr.clone()));
        }
    }
    let mut merged_exercises: Vec<serde_json::Value> = exercise_map.into_values().collect();
    merged_exercises.sort_by_key(|v| v.get("start").and_then(|s| s.as_i64()).unwrap_or(0));
    info!("activities after merge: {} total records", merged_exercises.len());

    let mut doc = serde_json::json!({
        "lastProcessedReading": last_processed.and_utc().timestamp(),
        "sleep_cycles": merged_sleeps,
        "activities": merged_exercises,
    });


    if let Some(stress) = stress_score {
        if let Some(obj) = doc.as_object_mut() {
            obj.insert("stress_score".to_string(), serde_json::json!({
                "time": stress.time.and_utc().timestamp(),
                "score": stress.score,
                "waking_rmssd_ms": stress.waking_rmssd,
                "baseline_hrv_ms": stress.baseline_hrv,
            }));
        }
    }

    // Add the most recent sleep's HRV stats at the top level of the document
    if let Some(latest_sleep) = sleeps.last() {
        if let Some(obj) = doc.as_object_mut() {
            obj.insert("latest_sleep_hrv".to_string(), serde_json::json!({
                "min_hrv": latest_sleep.min_hrv as f64,
                "max_hrv": latest_sleep.max_hrv as f64,
                "avg_hrv": latest_sleep.avg_hrv as f64,
                "avg_resp_rate": latest_sleep.avg_resp_rate,
                "sleep_start": latest_sleep.start.and_utc().timestamp(),
                "sleep_end": latest_sleep.end.and_utc().timestamp(),
            }));
        }
    }

    // Collect the top-level keys being written so Firestore performs a partial
    // update (update mask / PATCH semantics) and leaves all other existing
    // fields on the document untouched.
    let written_fields: Vec<String> = doc
        .as_object()
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default();

    firestore_db.fluent()
        .update()
        .fields(written_fields.iter().map(|s| s.as_str()))
        .in_col(&users_col)
        .document_id(user_id)
        .object(&doc)
        .execute::<()>()
        .await?;

    Ok(())
}

async fn scan_command(
    adapter: &Adapter,
    device_id: Option<DeviceId>,
) -> anyhow::Result<Peripheral> {
    adapter
        .start_scan(ScanFilter {
            services: vec![WHOOP_SERVICE],
        })
        .await?;

    loop {
        let peripherals = adapter.peripherals().await?;

        for peripheral in peripherals {
            let Some(properties) = peripheral.properties().await? else {
                continue;
            };

            if !properties.services.contains(&WHOOP_SERVICE) {
                continue;
            }

            let Some(device_id) = device_id.as_ref() else {
                println!("Address: {}", properties.address);
                println!("Name: {:?}", properties.local_name);
                println!("RSSI: {:?}", properties.rssi);
                println!();
                continue;
            };

            #[cfg(target_os = "linux")]
            if properties.address == *device_id {
                return Ok(peripheral);
            }

            #[cfg(target_os = "macos")]
            {
                let Some(name) = properties.local_name else {
                    continue;
                };
                if sanitize_name(&name).starts_with(device_id) {
                    return Ok(peripheral);
                }
            }
        }

        sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Clone, Copy, Debug)]
pub enum AlarmTime {
    DateTime(NaiveDateTime),
    Time(NaiveTime),
    Minute,
    Minute5,
    Minute10,
    Minute15,
    Minute30,
    Hour,
}

impl FromStr for AlarmTime {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(t) = s.parse() {
            return Ok(Self::DateTime(t));
        }

        if let Ok(t) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            return Ok(Self::DateTime(t));
        }

        if let Ok(t) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
            return Ok(Self::DateTime(t));
        }

        if let Ok(t) = s.parse() {
            return Ok(Self::Time(t));
        }

        match s {
            "minute" | "1min" | "min" => Ok(Self::Minute),
            "5minute" | "5min" => Ok(Self::Minute5),
            "10minute" | "10min" => Ok(Self::Minute10),
            "15minute" | "15min" => Ok(Self::Minute15),
            "30minute" | "30min" => Ok(Self::Minute30),
            "hour" | "h" => Ok(Self::Hour),
            _ => Err(anyhow!("Invalid alarm time")),
        }
    }
}

impl AlarmTime {
    pub fn unix(self) -> DateTime<Utc> {
        let mut now = Utc::now();
        let timezone_df = Local::now().offset().to_owned();

        match self {
            AlarmTime::DateTime(dt) => dt.and_utc() - timezone_df,
            AlarmTime::Time(t) => {
                let current_time = now.time();
                if current_time > t {
                    now += TimeDelta::days(1);
                }

                now.with_time(t).unwrap() - timezone_df
            }
            _ => {
                let offset = self.offset();
                now + offset
            }
        }
    }

    fn offset(self) -> TimeDelta {
        match self {
            AlarmTime::DateTime(_) => todo!(),
            AlarmTime::Time(_) => todo!(),
            AlarmTime::Minute => TimeDelta::minutes(1),
            AlarmTime::Minute5 => TimeDelta::minutes(5),
            AlarmTime::Minute10 => TimeDelta::minutes(10),
            AlarmTime::Minute15 => TimeDelta::minutes(15),
            AlarmTime::Minute30 => TimeDelta::minutes(30),
            AlarmTime::Hour => TimeDelta::hours(1),
        }
    }
}

#[cfg(target_os = "macos")]
pub fn sanitize_name(name: &str) -> String {
    name.chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .trim()
        .to_string()
}

impl OpenWhoopCli {
    async fn create_ble_adapter(&self) -> anyhow::Result<Adapter> {
        let manager = Manager::new().await?;

        #[cfg(target_os = "linux")]
        match self.ble_interface.as_ref() {
            Some(interface) => Self::adapter_from_name(&manager, interface).await,
            None => Self::default_adapter(&manager).await,
        }

        #[cfg(target_os = "macos")]
        Self::default_adapter(&manager).await
    }

    async fn adapter_from_name(manager: &Manager, interface: &str) -> anyhow::Result<Adapter> {
        let adapters = manager.adapters().await?;
        let mut c_adapter = Err(anyhow!("Adapter: `{}` not found", interface));
        for adapter in adapters {
            let name = adapter.adapter_info().await?;
            if name.starts_with(interface) {
                c_adapter = Ok(adapter);
                break;
            }
        }

        c_adapter
    }

    async fn default_adapter(manager: &Manager) -> anyhow::Result<Adapter> {
        let adapters = manager.adapters().await?;
        adapters
            .into_iter()
            .next()
            .ok_or(anyhow!("No BLE adapters found"))
    }
}

/// Aggregate heart rate readings within [start, end] into per-minute buckets.
///
/// Returns a chronologically sorted array of objects:
/// `{ "t": <unix_seconds at minute boundary>, "min": u8, "max": u8, "avg": f64 }`
///
/// Zero-BPM readings (band off) are excluded.  The "t" value is the Unix
/// timestamp of the start of each 60-second window (floor to minute).
fn aggregate_hr_by_minute(
    readings: &[whoop::ParsedHistoryReading],
    start: chrono::NaiveDateTime,
    end: chrono::NaiveDateTime,
) -> Vec<serde_json::Value> {
    use std::collections::BTreeMap;

    // (minute_unix → (min_bpm, max_bpm, sum, count))
    let mut by_minute: BTreeMap<i64, (u8, u8, u32, u32)> = BTreeMap::new();

    for r in readings
        .iter()
        .filter(|r| r.time >= start && r.time <= end && r.bpm > 0)
    {
        let minute = r.time.and_utc().timestamp() / 60 * 60;
        let entry = by_minute
            .entry(minute)
            .or_insert((r.bpm, r.bpm, 0, 0));
        entry.0 = entry.0.min(r.bpm);
        entry.1 = entry.1.max(r.bpm);
        entry.2 += r.bpm as u32;
        entry.3 += 1;
    }

    by_minute
        .into_iter()
        .map(|(minute, (min_bpm, max_bpm, sum, count))| {
            serde_json::json!({
                "t":   minute,
                "min": min_bpm,
                "max": max_bpm,
                "avg": (sum as f64 / count as f64 * 10.0).round() / 10.0,
            })
        })
        .collect()
}
