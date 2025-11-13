import requests
import json
from datetime import datetime
from snowflake.snowpark import Session
import sys
import pytz
import logging

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')

# Set the IST time zone
ist_timezone = pytz.timezone('Asia/Kolkata')

# Get the current time in IST
current_time_ist = datetime.now(ist_timezone)

# Format the timestamp
timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')

# Create the file name
file_name = f'air_quality_data_{timestamp}.json'

today_string = current_time_ist.strftime('%Y_%m_%d')

# Following credential has to come using secret whie running in automated way
def snowpark_basic_auth() -> Session:
    connection_parameters = {
       "ACCOUNT":"LXYUPGO-WC96196",
        "USER":"ADMIN",
        "PASSWORD":"Hakkoda@1234567890",
        "ROLE":"ACCOUNTADMIN",
        "DATABASE":"dev_db",
        "SCHEMA":"stage_sch",
        "WAREHOUSE":"load_wh"
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()


def get_air_quality_data(api_key, limit):
    api_url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'
    
    all_records = []
    offset = 0
    
    while True:
        params = {
            'api-key': api_key,
            'format': 'json',
            'limit': limit,
            'offset': offset
        }
        headers = {'accept': 'application/json'}
        
        try:
            response = requests.get(api_url, params=params, headers=headers)
            logging.info(f'Getting data batch with offset: {offset}')
            
            if response.status_code != 200:
                logging.error(f"Error: {response.status_code} - {response.text}")
                sys.exit(1)
            
            json_data = response.json()
            batch_records = json_data.get('records', [])
            
            all_records.extend(batch_records)
            
            total_records = int(json_data.get('total', 0))
            offset += limit
            
            logging.info(f"Batch size: {len(batch_records)}; Total collected: {len(all_records)} of {total_records}")
            
            if offset >= total_records:
                logging.info('All records fetched')
                break

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            sys.exit(1)
    
    # Save full collected data to file
    with open(file_name, 'w') as json_file:
        json.dump({'records': all_records}, json_file, indent=2)
    logging.info(f'All data written to local file: {file_name}')
    
    # Proceed with Snowflake file upload stage as in your existing code
    stg_location = '@dev_db.stage_sch.raw_stg/india/'
    sf_session = snowpark_basic_auth()
    logging.info(f'Uploading the file {file_name} to Snowflake stage {stg_location}')
    sf_session.file.put(file_name, stg_location)
    logging.info('JSON File placed successfully in Snowflake stage location')

    lst_query = f'list {stg_location}{file_name}.gz'
    logging.info(f'Listing files in Snowflake stage to verify placement: {lst_query}')
    result_lst = sf_session.sql(lst_query).collect()
    logging.info(f'Stage files: {result_lst}')
    
    logging.info('Job completed successfully.')
    return {'records': all_records}


# Replace 'YOUR_API_KEY' with your actual API key
api_key = '579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b'


limit_value = 4000
air_quality_data = get_air_quality_data(api_key, limit_value)
