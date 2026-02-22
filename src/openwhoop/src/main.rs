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

            if all_readings.is_empty() {
                println!("No data found for user {}", user_id);
                return Ok(());
            }

            let events = openwhoop_algos::ActivityPeriod::detect(&mut all_readings);
            let mut sleeps = Vec::new();
            let mut exercises = Vec::new();

            for event in events {
                match event.activity {
                    whoop::Activity::Sleep => {
                        let cycle = openwhoop_algos::SleepCycle::from_event(event, &all_readings);
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

            // Calculate stress for only the last 12 hours
            let twelve_hours_ago = chrono::Utc::now().naive_utc() - chrono::TimeDelta::hours(12);
            let recent_readings: Vec<_> = all_readings
                .iter()
                .filter(|r| r.time >= twelve_hours_ago)
                .cloned()
                .collect();

            let stress_score = openwhoop_algos::StressCalculator::calculate_stress(&recent_readings);
            if let Some(stress) = &stress_score {
                println!("Calculated Stress Score: {}", stress.score);
            } else {
                println!("Not enough recent data to calculate stress score");
            }

            println!("Saving User Data to Firestore...");
            let last_processed = all_readings.last().map(|r| r.time).unwrap_or_default();
            write_firestore_events(&user_id, &sleeps, &exercises, stress_score, last_processed).await?;
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
         ORDER BY unix ASC"
    );

    let mut query_request = gcp_bigquery_client::model::query_request::QueryRequest::new(query);
    query_request.use_legacy_sql = false;

    let mut response = bq_client
        .job()
        .query(&project_id, query_request)
        .await
        .map_err(|e| anyhow::anyhow!("BigQuery query failed: {}", e))?;

    let mut readings = Vec::new();

    loop {
        let mut rs = gcp_bigquery_client::model::query_response::ResultSet::new_from_query_response(response.clone());
        
        while rs.next_row() {
            if let (Ok(Some(time_str)), Ok(Some(bpm_i)), Ok(Some(rr_intervals)), Ok(Some(activity_i))) = (
                rs.get_string(0),
                rs.get_i64(1),
                rs.get_string(2),
                rs.get_i64(3),
            ) {
                let time = match chrono::DateTime::parse_from_rfc3339(&time_str) {
                    Ok(t) => t.naive_utc(),
                    Err(_) => {
                        chrono::NaiveDateTime::parse_from_str(&time_str, "%Y-%m-%d %H:%M:%S UTC")
                            .unwrap_or_default()
                    }
                };

                let hr = whoop::ParsedHistoryReading {
                    time,
                    bpm: bpm_i as u8,
                    rr: rr_intervals.split(',').filter_map(|s| s.parse().ok()).collect(),
                    activity: whoop::Activity::from(activity_i),
                };
                readings.push(hr);
            }
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

    info!("Fetched {} readings from BigQuery for user {}", readings.len(), user_id);
    Ok(readings)
}

async fn write_firestore_events(
    user_id: &str,
    sleeps: &[openwhoop_algos::SleepCycle],
    exercises: &[openwhoop_algos::ActivityPeriod],
    stress_score: Option<openwhoop_algos::StressScore>,
    last_processed: chrono::NaiveDateTime,

) -> anyhow::Result<()> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap_or_else(|_| "your-project-id".to_string());
    let users_col = std::env::var("FIRESTORE_COLLECTION_USERS").unwrap_or_else(|_| "users".to_string());

    let firestore_db = firestore::FirestoreDb::new(&project_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to init Firestore client: {}", e))?;

    let json_sleeps: Vec<_> = sleeps.iter().map(|sleep| serde_json::json!({
        "sleep_id": sleep.start.and_utc().timestamp(),
        "start": sleep.start.and_utc().timestamp(),
        "end": sleep.end.and_utc().timestamp(),
        "min_bpm": sleep.min_bpm,
        "max_bpm": sleep.max_bpm,
        "avg_bpm": sleep.avg_bpm as f64,
        "min_hrv": sleep.min_hrv as f64,
        "max_hrv": sleep.max_hrv as f64,
        "avg_hrv": sleep.avg_hrv as f64,
        "score": sleep.score,
    })).collect();

    let json_exercises: Vec<_> = exercises.iter().map(|exercise| serde_json::json!({
        "period_id": exercise.start.and_utc().timestamp(),
        "activity": exercise.activity as u8,
        "start": exercise.start.and_utc().timestamp(),
        "end": exercise.end.and_utc().timestamp(),
    })).collect();

    let mut doc = serde_json::json!({
        "lastProcessedReading": last_processed.and_utc().timestamp(),
        "sleep_cycles": json_sleeps,
        "activities": json_exercises,
    });

    if let Some(stress) = stress_score {
        if let Some(obj) = doc.as_object_mut() {
            obj.insert("stress_score".to_string(), serde_json::json!({
                "time": stress.time.and_utc().timestamp(),
                "score": stress.score as f64,
            }));
        }
    }

    firestore_db.fluent()
        .update()
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
