import json
import pandas as pd
import boto3
import ast
from io import StringIO
import s3_file_operations as s3_ops

def load_data_from_s3(bucket, key):
    
    print(f"Loading data from {key} in bucket {bucket}...")
    df = s3_ops.read_csv_from_s3(bucket, key)
    if df is None:
        raise ValueError(f"Failed to load data from {key}")
    print(f"Loaded data with shape: {df.shape}")
    return df

def transform_characters(characters_df):
    
    print("Transforming Characters DataFrame...")
    characters_df['origin_id'] = characters_df['origin'].apply(lambda x: extract_id(x))
    characters_df['location_id'] = characters_df['location'].apply(lambda x: extract_id(x))
    return characters_df.drop(columns=['origin', 'location', 'episode'])

def transform_episodes(episodes_df):
    
    print("Transforming Episodes DataFrame...")
    appearance_df = episodes_df.copy()
    
    # Extract character IDs
    appearance_df['character_ids'] = appearance_df['characters'].apply(lambda x: extract_character_ids(x))
    
    # Explode the DataFrame to normalize many-to-many relationship
    expanded_df = appearance_df.explode('character_ids').reset_index(drop=True)
    
    # Adding the 'episode_id' column before renaming
    expanded_df['episode_id'] = expanded_df.index + 1
    
    # Reset index to create a new unique ID for each row
    expanded_df = expanded_df.reset_index().rename(columns={'index': 'id'})
    
    # Select and rename columns appropriately
    expanded_df = expanded_df[['id', 'episode_id', 'character_ids']]
    expanded_df = expanded_df.rename(columns={'character_ids': 'character_id'})
    
    # Drop the 'characters' column from episodes_df as it's now normalized
    episodes_df = episodes_df.drop("characters", axis=1)
    
    return expanded_df, episodes_df


def transform_locations(location_df):
    
    print("Transforming Locations DataFrame...")
    return location_df.drop('residents', axis=1)

def extract_id(record):
    
    if isinstance(record, str):
        parsed_record = ast.literal_eval(record)
        return parsed_record['url'].split('/')[-1] if 'url' in parsed_record else None
    return None

def extract_character_ids(record):
    
    if isinstance(record, str):
        return [url.split('/')[-1] for url in ast.literal_eval(record)]
    return None

def save_transformed_data(bucket, transformed_dfs, paths):
   
    for df, path in zip(transformed_dfs, paths):
        s3_ops.write_data_to_s3(df, bucket, path)
        print(f"Saved transformed data to {path}")

def lambda_handler(event, context):
    bucket = "de-masterclass-mutai"  # S3 bucket name
    print("Starting data transformation process...")

    try:
        # Load data from S3
        characters_df = load_data_from_s3(bucket, 'Rick&Morty/Untransformed/Character.csv')
        episodes_df = load_data_from_s3(bucket, 'Rick&Morty/Untransformed/Episode.csv')
        location_df = load_data_from_s3(bucket, 'Rick&Morty/Untransformed/Location.csv')

        # Transform data
        transformed_characters_df = transform_characters(characters_df)
        appearance_df, transformed_episodes_df = transform_episodes(episodes_df)
        transformed_location_df = transform_locations(location_df)

        # Save transformed data to S3
        save_transformed_data(bucket, 
                              [transformed_characters_df, transformed_episodes_df, appearance_df, transformed_location_df],
                              ['Rick&Morty/Transformed/Character.csv',
                               'Rick&Morty/Transformed/Episode.csv',
                               'Rick&Morty/Transformed/Appearance.csv',
                               'Rick&Morty/Transformed/Location.csv'])

        return {
            'statusCode': 200,
            'body': json.dumps('Data transformation and upload successful')
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Data transformation and upload failed')
        }
