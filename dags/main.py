from airflow.models import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from zipfile import ZipFile 
import pandas as pd
import json
from google.cloud import bigquery
import requests
import aiohttp
import asyncio
import math

client = bigquery.Client()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["lojingjiedylan@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='is3107_groupproject', default_args=default_args, schedule=None, catchup=False, tags=['is3107'])

# HELPER FUNCTIONS
def onemap_api(data):
    async def fetch(session, blk_no, street):
        search_val = f"{blk_no} {street}"
        url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={search_val}&returnGeom=Y&getAddrDetails=Y&pageNum=1"

        async with session.get(url) as response:
            try:
                query = await response.json()
                query = query['results'][0]  
                postal = query['POSTAL']
                return postal
            except:
                print('API Query failed')
                return None
            
    async def main():
        tasks = []
        async with aiohttp.ClientSession() as session:
            for index, row in data.iterrows():
                task = asyncio.create_task(fetch(session, row['blk_no'], row['street']))
                tasks.append(task)
            results = await asyncio.gather(*tasks)
        return results

    return asyncio.run(main())


# DAG
def assignment3_dag():
    def extract_raw_data():
        return asyncio.run(extract_raw_data_async())

    async def extract_raw_data_async():
        data_gov_base_url = "https://data.gov.sg/api/action/datastore_search?resource_id="
        headers = {"User-Agent": "Mozilla/5.0"}
        datagov_name_and_ids = [
            ("buildings", "d_17f5382f26140b1fdae0ba2ef6239d2f"),
            ("transactions1_data", "d_2d5ff9ea31397b66239f245f57751537"),
            ("transactions2_data", "d_8b84c4ee58e3cfc0ece0d773c8ca6abc")
        ]

        async def datagovsg_id_to_csv_async(resource_id, filename, row_max_limit=5000, iterations=10):
            try:
                base_url = f"{data_gov_base_url}{resource_id}"
                check = requests.get(base_url, headers=headers)
                record_count = check.json()["result"]["total"]
            except KeyError:
                print(f"Invalid result response, check the url: {base_url}")
                return None
            except Exception as e:
                print(f"Failed to access the API at url: {base_url}")
                print(e)
                return None
            
            async def fetch(s, url):
                async with s.get(url) as result:
                    if result.status != 200:
                        result.raise_for_status()
                    output = await result.json()
                    try:
                        output = output["result"]["records"]
                        return output
                    except:
                        print(f"Error downloading at at url: {url}")
                        print(output)

            async def fetch_all(s, urls):
                tasks = []
                for url in urls:
                    task = asyncio.create_task(fetch(s, url))
                    tasks.append(task)
                completed = await asyncio.gather(*tasks)
                result = []
                for complete in completed:
                    result.extend(complete)
                return result

            rows_per_iteration = min(math.ceil(record_count / iterations), row_max_limit)
            num_calls = math.ceil(record_count / rows_per_iteration)

            urls = [f"{base_url}&offset={i * rows_per_iteration}&limit={rows_per_iteration}" for i in range(0, num_calls)]
            async with aiohttp.ClientSession(headers=headers) as s:
                downloaded_records = await fetch_all(s, urls)
            
            if len(downloaded_records) != record_count:
                print(f"Dataset {resource_id} ({filename}) did not download completely.")
                print(f"Expected row counts: {record_count}")
                print(f"Downloaded row counts: {len(downloaded_records)}")

            pd.DataFrame.from_records(downloaded_records).to_csv(f"{filename}.csv", index=False)
            return 
            
        tasks = []
        for filename, dataset_id in datagov_name_and_ids:
            task = asyncio.create_task(datagovsg_id_to_csv_async(dataset_id, filename))
            tasks.append(task)
        await asyncio.gather(*tasks)

        return [filename for filename, _ in datagov_name_and_ids]
    
    @task
    def test_transform(filenames):
        
        return

    # Task dependencies
    raw_data = extract_raw_data()
    test_transform(raw_data)
    return
    
assignment3_etl = assignment3_dag()
