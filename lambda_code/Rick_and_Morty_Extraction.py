import requests
import pandas as pd
import json
import datetime
import s3_file_operations as s3_ops

def fetch_data_from_api(api_url):
   
    page = 1
    next = True
    all_data = []

    while True:
        response = requests.get(f"{api_url}?page={page}")
        
        # Ensure request was successful
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")
        
        # Extract data
        data = response.json().get('results', [])
        all_data.extend(data)

        # Check if there's a next page
        if not response.json().get('info', {}).get("next"):
            break
        
        page += 1

    return all_data

def convert_data_to_dataframe(data):
     
    return pd.DataFrame(data)

def save_dataframe_to_s3(df, bucket, key):
   
   
    s3_ops.write_data_to_s3(df, bucket_name=bucket, key=key)

def process_table(api_url, table_name, bucket):
     
    print(f"Processing table: {table_name}")
    data = fetch_data_from_api(api_url)
    df = convert_data_to_dataframe(data)
    save_dataframe_to_s3(df, bucket, f"Rick&Morty/Untransformed/{table_name}.csv")
    print(f"Data for {table_name} successfully saved to S3.")

def lambda_handler(event, context):
   
    
    print("Starting extraction process...")
    
    bucket = "de-masterclass"  # S3 bucket name

    tables = {
        "Character": "https://rickandmortyapi.com/api/character",
        "Location": "https://rickandmortyapi.com/api/location",
        "Episode": "https://rickandmortyapi.com/api/episode"
    }

    for table_name, api_url in tables.items():
        try:
            process_table(api_url, table_name, bucket)
        except Exception as e:
            print(f"Error processing {table_name}: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Failed to process {table_name}: {str(e)}")
            }

    return {
        'statusCode': 200,
        'body': json.dumps('All data successfully processed and saved to S3!')
    }
