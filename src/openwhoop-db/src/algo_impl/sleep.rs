use db_entities::sleep_cycles;
use openwhoop_algos::SleepCycle;
use sea_orm::{EntityTrait, QueryOrder};

use crate::DatabaseHandler;

impl DatabaseHandler {
    pub async fn get_sleep_cycles(&self) -> anyhow::Result<Vec<SleepCycle>> {
        Ok(sleep_cycles::Entity::find()
            .order_by_asc(sleep_cycles::Column::Start)
            .all(&self.db)
            .await?
            .into_iter()
            .map(map_sleep_cycle)
            .collect())
    }
}

fn map_sleep_cycle(value: sleep_cycles::Model) -> SleepCycle {
    SleepCycle {
        id: value.sleep_id,
        start: value.start,
        end: value.end,
        min_bpm: value.min_bpm.try_into().unwrap(),
        max_bpm: value.max_bpm.try_into().unwrap(),
        avg_bpm: value.avg_bpm.try_into().unwrap(),
        min_hrv: value.min_hrv.try_into().unwrap(),
        max_hrv: value.max_hrv.try_into().unwrap(),
        avg_hrv: value.avg_hrv.try_into().unwrap(),
        avg_resp_rate: 0.0, // not stored in SQLite; computed only in BigQuery path
        score: value
            .score
            .unwrap_or(SleepCycle::sleep_score(value.start, value.end)),
    }
}
