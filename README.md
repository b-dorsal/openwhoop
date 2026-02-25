# OpenWhoop

OpenWhoop is project that allows you to download heart rate data directly from your Whoop4.0 device without Whoop subscription or Whoops servers, making data your own.

### How to Run?

First you need to copy `.env.example` into `.env` and then scan until you find your Whoop device:
```sh
cp .env.example .env
cargo run -r -- scan
```

After you find your device:

On linux copy its address to `.env` under `WHOOP`, 

On macos copy its name to `.env` under `WHOOP`,  

After that you can download data from your whoop by running:
```sh
cargo run -r -- download-history
```

If you are using macos you should 

### Importing data to python

If you want to import heart rate data into python you can use following code:
```py
import pandas as pd
import os

QUERY = "SELECT time, bpm from heart_rate"
PREFIX = "sqlite:///" # This is prefix if you are working in same dir as `.env` if you are working in `notebooks/` change to `sqlite:///../`
DATABASE_URL = os.getenv("DATABASE_URL").replace("sqlite://", PREFIX) 
df = pd.read_sql(QUERY, DATABASE_URL)
```

### Processing Data Locally

To test the data processing logic locally for a specific user, you can run the `process-user` CLI command with the necessary BigQuery environment variables overriding the defaults. Replace the user ID string with the target user's ID:

```sh
export GOOGLE_CLOUD_PROJECT=openwhoops
export BIGQUERY_DATASET=openwhoop_analytics
export LOOKBACK_HOURS=36
export RUST_LOG=info

cargo run --bin openwhoop --release -- process-user "klEddRd1tqNHQV8gkC5mOR6X18O2"
```

Or as a single line:

```sh
GOOGLE_CLOUD_PROJECT=openwhoops BIGQUERY_DATASET=openwhoop_analytics LOOKBACK_HOURS=36 RUST_LOG=info cargo run --release --bin openwhoop -- process-user "klEddRd1tqNHQV8gkC5mOR6X18O2"
```

## TODO:

- [ ] Sleep detection, for most of things like strain, recovery, HRV, etc..., I have been able to reverse engineer calculations, but I need reverse engineer sleep detection and activity detection before they can be automatically calculated
- [ ] Mobile/Desktop app
- [ ] Sp02 readings
- [ ] Temperature readings