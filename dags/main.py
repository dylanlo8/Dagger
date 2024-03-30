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
import re
import numpy as np

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

# HELPER FUNCTIONS
async def extract_raw_data_async():
    data_gov_base_url = "https://data.gov.sg/api/action/datastore_search?resource_id="
    headers = {"User-Agent": "Mozilla/5.0"}

    datagov_name_and_ids = [
        ("buildings", "d_17f5382f26140b1fdae0ba2ef6239d2f"),
        ("transactions1_data", "d_ebc5ab87086db484f88045b47411ebc5"),
        ("transactions2_data", "d_43f493c6c50d54243cc1eab0df142d6a"),
        ("transactions3_data", "d_2d5ff9ea31397b66239f245f57751537"),
        ("transactions4_data", "d_ea9ed51da2787afaf8e51f827c304208"),
        ("transactions5_data", "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"),
        ("median_rent", "d_23000a00c52996c55106084ed0339566"),
        ("schools", "d_688b934f82c1059ed0a6993d2a829089")
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

def onemap_api_postal(data):
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
                print(f'API Query failed for {blk_no} , {street}')
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

town_mapping = {
        'AMK': 'ANG MO KIO', 'BB': 'BUKIT BATOK', 'BD': 'BEDOK', 'BH': 'BISHAN', 'BM': 'BUKIT MERAH',
        'BP': 'BUKIT PANJANG', 'BT': 'BUKIT TIMAH', 'CCK': 'CHOA CHU KANG', 'CL': 'CLEMENTI', 'CT': 'CENTRAL AREA',
        'GL': 'GEYLANG', 'HG': 'HOUGANG', 'JE': 'JURONG EAST', 'JW': 'JURONG WEST', 'KWN': 'KALLANG/WHAMPOA',
        'MP': 'MARINE PARADE', 'PG': 'PUNGGOL', 'PRC': 'PASIR RIS', 'QT': 'QUEENSTOWN', 'SB': 'SEMBAWANG',
        'SGN': 'SERANGOON', 'SK': 'SENGKANG', 'TAP': 'TAMPINES', 'TG': 'TENGAH', 'TP' : 'TOA PAYOH' ,
        'WL' : 'WOODLANDS' , 'YS' : 'YISHUN'
}


# DAG
@dag(dag_id='is3107_groupproject', default_args=default_args, schedule=None, catchup=False, tags=['is3107'])
def assignment3_dag():
    # Extract Tasks
    @task
    def extract_raw_data():
        return asyncio.run(extract_raw_data_async())

    # Read Data Tasks
    @task
    def read_buildings():
        return pd.read_csv('buildings.csv', index_col = 0).reset_index(drop = True)

    @task
    def read_schools():
        return pd.read_csv('schools.csv')

    @task
    def read_rent():
        return pd.read_csv('median_rent.csv')

    @task
    def read_transactions():
        trans1 = pd.read_csv('transactions1_data.csv', index_col=0).reset_index(drop = True)
        trans2 = pd.read_csv('transactions2_data.csv', index_col=0).reset_index(drop = True)
        trans3 = pd.read_csv('transactions3_data.csv', index_col=0).reset_index(drop = True)
        trans4 = pd.read_csv('transactions4_data.csv', index_col=0).reset_index(drop = True)
        trans5 = pd.read_csv('transactions5_data.csv', index_col=0).reset_index(drop = True)

        merged_transactions = pd.concat([trans1, trans2, trans3, trans4, trans5], ignore_index=True).reset_index()
        merged_transactions = merged_transactions.rename({'index': 'transaction_id'}, axis = 1)
        return merged_transactions

    # Transform Tasks
    # Buildings Entity
    @task
    def transform_buildings(buildings):
        # Run OneMap API to get postal code
        buildings['postal_code'] = onemap_api_postal(buildings)

        # Convert to the correct formatting for Town
        buildings['bldg_contract_town'] = buildings['bldg_contract_town'].map(town_mapping)

        # Remove irrelevant columns
        retained_cols = ['postal_code', 'blk_no', 'street', 'max_floor_lvl', 'year_completed', 
                         'multistorey_carpark', 'precinct_pavilion', 'market_hawker']
        
        buildings = buildings[retained_cols]
        return buildings

    # Flats Sold Entity
    @task
    def create_flats_sold(buildings):
        # Extracting required columns
        flat_type_df = pd.DataFrame()
        flat_type_df['postal_code'] = buildings['postal_code']
        
        # Extracting flat types and their respective sold counts
        renamed_flat_types = {'1room_sold': '1 ROOM', '2room_sold': '2 ROOM', '3room_sold': '3 ROOM',
                            '4room_sold': '4 ROOM', '5room_sold' : '5 ROOM', 'exec_sold' : 'EXECUTIVE', 
                            'multigen_sold' : 'MULTI-GENERATION'}

        # Creating new columns for each flat type and their sold counts
        for flat_type, renamed_flat_type in renamed_flat_types.items():
            flat_type_df[renamed_flat_type] = buildings[flat_type]
        
        # Reshaping the DataFrame to have flat_type as a column
        flat_type_df = pd.melt(flat_type_df, id_vars=['postal_code'], value_vars=list(renamed_flat_types.values()),
                                var_name='flat_type', value_name='flats_sold')
        
        return flat_type_df

    # Town Entity
    @task
    def create_town(town_mapping):
        return pd.DataFrame(list(town_mapping.values()), columns = ['town'])

    
    # Rent Entity
    @task
    def transform_median_rent(rent):
        renamed_flat_types = {'1-RM': '1 ROOM', '2-RM': '2 ROOM', '3-RM': '3 ROOM',
                            '4-RM': '4 ROOM', '5-RM' : '5 ROOM', 'EXEC' : 'EXECUTIVE'}   
                            
        # Splitting quarter column into year and quarter columns
        rent[['year', 'quarter']] = rent['quarter'].str.split('-', expand=True)
        rent['year'] = pd.to_numeric(rent['year'])
        
        # remove unnecessary trailing spaces
        rent = rent.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        # replace na values and - values with NaN
        rent.replace(r'^\s*na\s*$', np.nan, regex = True, inplace=True)
        rent.replace(r'^\s*-\s*$', np.nan, regex = True, inplace=True)
        
        # convert flat type to consistent format
        rent['flat_type'] = rent['flat_type'].map(renamed_flat_types)

        # reorder the columns
        rent = rent[['year', 'quarter', 'town', 'flat_type', 'median_rent']]
        return rent

    # Schools entity
    @task
    def transform_schools(schools):
        def clean_and_format_town_name(text):
            # Remove stopwords and make format better
            text = text.title()
            cleaned_text = re.sub(r'\b(?:Station|Mrt|Lrt|Line|\([^)]*\))\b', '', text)
            return cleaned_text.strip()
        
        schools['mrt_desc'] = schools['mrt_desc'].apply(lambda x: clean_and_format_town_name(x))

        retained_cols = ['school_name', 'postal_code', 'mrt_desc', 'mainlevel_code', 
                        'nature_code', 'type_code', 'sap_ind', 'autonomous_ind', 'gifted_ind', 'ip_ind']
        
        return schools[retained_cols].rename(columns={'mrt_desc': 'closest_mrt'})

    # Transactions entity
    @task
    def transform_transactions(transactions):
        # Drop the remaining_lease column
        transactions = transactions.drop(['remaining_lease'], axis = 1)

        # Apply formatting to flat_model
        transactions['flat_model'] = transactions['flat_model'].str.upper()

        # Convert 'MULTI GENERATION' to 'MULTI-GENERATION'
        transactions['flat_type'] = transactions['flat_type'].str.replace('MULTI GENERATION', 'MULTI-GENERATION')
        
        # rename lease_commence_date
        transactions = transactions.rename({'lease_commence_date' : 'lease_commence_year'}, axis = 1)
        return transactions
    
    # Flat Type Entity
    @task
    def create_flat_type(transactions):
        flat_type = pd.DataFrame(transactions['flat_type'].unique(), columns = ['flat_type'])
        return flat_type
    
    # MRT Stations Entity
    @task
    def create_mrt_stations():
        def onemap_api_mrt_stations():
            # Making simple request query to get totalNumPages
            search_val = "MRT Station"
            url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={search_val}&returnGeom=Y&getAddrDetails=Y&pageNum=1"
            response = requests.request("GET", url)
            num_pages = response.json()['totalNumPages']

            # Async Fetch function
            async def fetch(session, page_num):
                url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={search_val}&returnGeom=Y&getAddrDetails=Y&pageNum={page_num}"
                async with session.get(url) as response:
                    try:
                        query = await response.json()
                        query = query['results']
                        return query
                    except:
                        return None
            
            # Async main function to fetch all records
            async def main():
                tasks = []
                page_num = 1 # Starts with page 1 of query
                async with aiohttp.ClientSession() as session:
                    while page_num <= num_pages:
                        task = asyncio.create_task(fetch(session, page_num))
                        tasks.append(task)
                        page_num += 1
                    results = await asyncio.gather(*tasks)
                return results

            return asyncio.run(main())
    
        mrt_stations_query = onemap_api_mrt_stations()

        # Flatten nested list and filter get unique MRT Station names with (Station Code)
        stations = [(station['BUILDING'], station['POSTAL']) for sublist in mrt_stations_query for station in sublist if '(' in station['BUILDING'] and ')' in station['BUILDING']]
        
        # Convert stations into dataframe and remove duplicate postal_codes
        stations = pd.DataFrame(stations, columns = ['station_name', 'postal_code']).drop_duplicates(subset = ['postal_code'])
        return stations
    
    @task
    def test_load(data):
        i = 0
        for df in data:
            df.to_csv(f"{i}.csv")
            i += 1
        return

    # Task dependencies
    extract_raw_data()
    #buildings = transform_buildings(read_buildings())
    #flats_sold = create_flats_sold(buildings)
    #town = create_town(town_mapping)
    #rent = transform_median_rent(read_rent())
    #schools = transform_schools(read_schools())
    #transactions = transform_transactions(read_transactions())
    #flat_type = create_flat_type(transactions)
    #mrt_stations = create_mrt_stations()

    #test_load([buildings, flats_sold, town, rent, schools, transactions, flat_type, mrt_stations])
    return

assignment3_etl = assignment3_dag()
