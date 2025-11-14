import requests
import json
from datetime import datetime
from snowflake.snowpark import Session
import sys
import pytz
import logging

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')

# Constants
API_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"
BASE_URL = "https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
LIMIT = 100  # records per call


# -------------------------------
# 1️⃣ Snowflake Connection
# -------------------------------
def snowpark_basic_auth() -> Session:
    connection_parameters = {
        "ACCOUNT": "LXYUPGO-WC96196",
        "USER": "ADMIN",
        "PASSWORD": "Hakkoda@1234567890",
        "ROLE": "ACCOUNTADMIN",
        "DATABASE": "dev_db",
        "SCHEMA": "stage_sch",
        "WAREHOUSE": "load_wh"
    }
    return Session.builder.configs(connection_parameters).create()


# -------------------------------
# 2️⃣ Fetch ALL paginated API data (NO FILTER)
# -------------------------------
def fetch_all_data(api_key):
    offset = 0
    all_records = []

    logging.info("🔎 Fetching ALL records from API using pagination...")

    while True:
        params = {
            "api-key": api_key,
            "format": "json",
            "limit": LIMIT,
            "offset": offset
        }

        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        records = data.get('records', [])
        all_records.extend(records)

        logging.info(f"✔️ Retrieved {len(records)} records... offset={offset}")

        total_records = int(data.get("total", 0))
        offset += LIMIT

        if offset >= total_records:
            break

    logging.info(f"🎯 Total records fetched = {len(all_records)}")
    return all_records


# -------------------------------
# 3️⃣ Save data to local file & upload to Snowflake stage
# -------------------------------
def upload_to_snowflake_stage(records, filename, today_string):
    sf_session = snowpark_basic_auth()

    # Save locally
    with open(filename, "w") as json_file:
        json.dump(records, json_file, indent=2)

    logging.info(f"💾 File saved locally: {filename}")

    # Upload to stage
    stage_path = f"@dev_db.stage_sch.raw_stg/india"
    logging.info(f"🚀 Uploading to Snowflake stage: {stage_path}")

    sf_session.file.put(filename, stage_path)

    # Validate upload
    check_query = f"list {stage_path}{filename}.gz"
    result = sf_session.sql(check_query).collect()

    logging.info(f"📂 File present in stage: {result}")


# -------------------------------
# 4️⃣ Main Execution
# -------------------------------
if __name__ == "__main__":

    # IST timestamp
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)

    today_string = now.strftime("%Y_%m_%d")
    timestamp = now.strftime("%Y_%m_%d_%H_%M_%S")

    file_name = f"air_quality_full_{timestamp}.json"

    # Step 1 → fetch ALL records (NO FILTER)
    all_data = fetch_all_data(API_KEY)

    # Step 2 → upload JSON to Snowflake stage
    upload_to_snowflake_stage(all_data, file_name, today_string)

    logging.info("🎉 Job completed successfully!")
