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
import pandas_gbq


client = bigquery.Client()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["lojingjiedylan@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

# Variables
datagov_name_and_id_buildings = ("buildings", "d_17f5382f26140b1fdae0ba2ef6239d2f")
datagov_name_and_id_median_rent = ("median_rent", "d_23000a00c52996c55106084ed0339566")
datagov_name_and_id_schools = ("schools", "d_688b934f82c1059ed0a6993d2a829089")
datagov_name_and_id_trans1 = ("transactions1_data", "d_ebc5ab87086db484f88045b47411ebc5")
datagov_name_and_id_trans2 = ("transactions2_data", "d_43f493c6c50d54243cc1eab0df142d6a")
datagov_name_and_id_trans3 = ("transactions3_data", "d_2d5ff9ea31397b66239f245f57751537")
datagov_name_and_id_trans4 = ("transactions4_data", "d_ea9ed51da2787afaf8e51f827c304208")
datagov_name_and_id_trans5 = ("transactions5_data", "d_8b84c4ee58e3cfc0ece0d773c8ca6abc")

town_mapping = {
    'AMK': 'ANG MO KIO', 'BB': 'BUKIT BATOK', 'BD': 'BEDOK', 'BH': 'BISHAN', 'BM': 'BUKIT MERAH',
    'BP': 'BUKIT PANJANG', 'BT': 'BUKIT TIMAH', 'CCK': 'CHOA CHU KANG', 'CL': 'CLEMENTI', 'CT': 'CENTRAL AREA',
    'GL': 'GEYLANG', 'HG': 'HOUGANG', 'JE': 'JURONG EAST', 'JW': 'JURONG WEST', 'KWN': 'KALLANG/WHAMPOA',
    'MP': 'MARINE PARADE', 'PG': 'PUNGGOL', 'PRC': 'PASIR RIS', 'QT': 'QUEENSTOWN', 'SB': 'SEMBAWANG',
    'SGN': 'SERANGOON', 'SK': 'SENGKANG', 'TAP': 'TAMPINES', 'TG': 'TENGAH', 'TP' : 'TOA PAYOH' ,
    'WL' : 'WOODLANDS' , 'YS' : 'YISHUN'
}

project_id = 'is3107-415212'

# HELPER FUNCTIONS
async def extract_raw_data_async(resource_id):
    data_gov_base_url = "https://data.gov.sg/api/action/datastore_search?resource_id="
    try:
        base_url = f"{data_gov_base_url}{resource_id}"
        check = requests.get(base_url)
        record_count = check.json()["result"]["total"]
    except KeyError:
        print(f"Invalid result response, check the url: {base_url}")
        return None
    except Exception as e:
        print(f"Failed to access the API at url: {base_url}")
        print(e)
        return None

    async with aiohttp.ClientSession() as session:
        async def fetch(url):
            async with session.get(url) as response:
                if response.status != 200:
                    response.raise_for_status()
                return await response.json()

        rows_per_iteration = min(math.ceil(record_count / 10), 5000)
        num_calls = math.ceil(record_count / rows_per_iteration)

        urls = [f"{base_url}&offset={i * rows_per_iteration}&limit={rows_per_iteration}" for i in range(num_calls)]
        tasks = [fetch(url) for url in urls]
        downloaded_records = await asyncio.gather(*tasks)

    return [record for call in downloaded_records for record in call['result']['records']]

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

# DAG
@dag(dag_id='is3107', default_args=default_args, schedule=None, catchup=False, tags=['is3107'])
def dag():
    # Extract Tasks
    @task
    def extract_trans1():
        records = asyncio.run(extract_raw_data_async(datagov_name_and_id_trans1[1]))
        df = pd.DataFrame(records)
        return df
    
    @task
    def extract_trans2():
        records = asyncio.run(extract_raw_data_async(datagov_name_and_id_trans2[1]))
        df = pd.DataFrame(records)
        return df

    @task
    def extract_trans3():
        records = asyncio.run(extract_raw_data_async(datagov_name_and_id_trans3[1]))
        df = pd.DataFrame(records)
        return df    

    @task
    def extract_trans4():
        records = asyncio.run(extract_raw_data_async(datagov_name_and_id_trans4[1]))
        df = pd.DataFrame(records)
        return df

    @task
    def extract_trans5():
        records = asyncio.run(extract_raw_data_async(datagov_name_and_id_trans5[1]))
        df = pd.DataFrame(records)
        return df
    
    @task
    def extract_schools():
        records = asyncio.run(extract_raw_data_async(datagov_name_and_id_schools[1]))
        df = pd.DataFrame(records)
        df = df.drop(df.columns[0], axis=1)
        return df
    
    @task
    def extract_buildings():
        records = asyncio.run(extract_raw_data_async(datagov_name_and_id_buildings[1]))
        df = pd.DataFrame(records)
        df = df.drop(df.columns[0], axis=1)
        return df
    
    @task
    def extract_rent():
        records = asyncio.run(extract_raw_data_async(datagov_name_and_id_median_rent[1]))
        df = pd.DataFrame(records)
        df = df.drop(df.columns[0], axis=1)
        return df
    
   

    # Transform Tasks
    # Buildings Entity
    @task
    def transform_buildings(buildings):
        # Run OneMap API to get postal code 
        buildings['postal_code'] = onemap_api_postal(buildings)

        # Convert to the correct formatting for Town
        buildings['town'] = buildings['bldg_contract_town'].map(town_mapping)
        buildings = buildings.drop('bldg_contract_town', axis = 1)

        # map boolean columns into boolean values
        buildings[['multistorey_carpark', 'precinct_pavilion', 'market_hawker']] = buildings[['multistorey_carpark', 'precinct_pavilion', 'market_hawker']].apply(lambda x : True if x == 'Y' else False)

        return buildings
    
    @task
    def drop_building_columns(buildings):
        # Remove irrelevant columns
        retained_cols = ['postal_code', 'blk_no', 'street', 'max_floor_lvl', 'year_completed', 
                        'multistorey_carpark', 'precinct_pavilion', 'market_hawker', 'town']
        
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
    def merge_transactions(list_trans):
        merged_transactions = pd.concat(list_trans, ignore_index=True).reset_index()
        merged_transactions = merged_transactions.rename({'index': 'transaction_id'}, axis = 1)
        return merged_transactions
    
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
        mrt_stations_query = onemap_api_mrt_stations()

        # Flatten nested list and filter get unique MRT Station names with (Station Code)
        stations = [(station['BUILDING'], station['POSTAL']) for sublist in mrt_stations_query for station in sublist if '(' in station['BUILDING'] and ')' in station['BUILDING']]
        
        # Convert stations into dataframe and remove duplicate postal_codes
        stations = pd.DataFrame(stations, columns = ['station_name', 'postal_code']).drop_duplicates(subset = ['postal_code'])
        return stations
    

    # Load Functions
    @task
    def load_transactions(transactions):
        pandas_gbq.to_gbq(transactions, 'hdb.transactions', project_id = project_id)

    @task
    def load_mrt_stations(stations):
        pandas_gbq.to_gbq(stations, 'hdb.stations', project_id = project_id)

    @task
    def load_flat_type(flat_type):
        pandas_gbq.to_gbq(flat_type, 'hdb.flat_type', project_id = project_id)

    @task
    def load_town(town):
        pandas_gbq.to_gbq(town, 'hdb.town', project_id = project_id)
    
    @task
    def load_flats_sold(flats_sold):
        pandas_gbq.to_gbq(flats_sold, 'hdb.flats_sold', project_id = project_id)
    
    @task
    def load_buildings(buildings):
        pandas_gbq.to_gbq(buildings, 'hdb.buildings', project_id = project_id)
    
    @task
    def load_rent(rent):
        pandas_gbq.to_gbq(rent, 'hdb.rent', project_id = project_id)
    
    @task
    def load_schools(schools):
        pandas_gbq.to_gbq(schools, 'hdb.schools', project_id = project_id)


    
    # Task dependencies
    buildings = extract_buildings()
    buildings = transform_buildings(buildings)
    flats_sold = create_flats_sold(buildings)
    buildings = drop_building_columns(buildings)

    rent = extract_rent()
    rent = transform_median_rent(rent)

    schools = extract_schools()
    schools = transform_schools(schools)

    # trans1 = extract_trans1()
    # trans2 = extract_trans2()
    # trans3 = extract_trans3()
    # trans4 = extract_trans4()
    trans5 = extract_trans5()

    transactions = trans5
    transactions = transform_transactions(transactions)
    flat_type = create_flat_type(transactions)
    town = create_town(town_mapping)
    mrt_stations = create_mrt_stations()

    # Load Functions
    load_flats_sold(flats_sold)
    load_town(town)
    load_buildings(buildings)
    load_schools(schools)
    load_transactions(transactions)
    load_mrt_stations(mrt_stations)
    load_flat_type(flat_type)
    load_rent(rent)

    return

is3107_etl = dag()
