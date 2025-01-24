import json
import boto3
import os
from datetime import datetime
from io import StringIO
import pandas as pd
from typing import List, Dict, Any

# Constants
RAW_DATA_PATH = 'raw_data/to_processed/'
PROCESSED_DATA_PATH = 'raw_data/processed_data/'
TRANSFORMED_PATHS = {
    'songs': 'transformed_data/songs_data/',
    'albums': 'transformed_data/album_data/',
    'artists': 'transformed_data/artist_data/'
}

def generate_file_name(prefix: str, extension: str) -> str:
    """Generate a formatted filename with timestamp."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{prefix}_transformed_{timestamp}.{extension}"

def album(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract album information from Spotify data."""
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        
        album_elements = {
            'album_id': album_id,
            'name': album_name,
            'release_date': album_release_date,
            'total_tracks': album_total_tracks,
            'url': album_url
        }
        album_list.append(album_elements)
    return album_list

def artist(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract artist information from Spotify data."""
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_dict = {
                        'artist_id': artist['id'],
                        'artist_name': artist['name'],
                        'external_url': artist['href']
                    }
                    artist_list.append(artist_dict)
    return artist_list

def songs(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract song information from Spotify data."""
    song_list = []
    for row in data['items']:
        song_element = {
            'song_id': row['track']['id'],
            'song_name': row['track']['name'],
            'duration_ms': row['track']['duration_ms'],
            'url': row['track']['external_urls']['spotify'],
            'popularity': row['track']['popularity'],
            'song_added': row['added_at'],
            'album_id': row['track']['album']['id'],
            'artist_id': row['track']['album']['artists'][0]['id']
        }
        song_list.append(song_element)
    return song_list

def process_and_write_dataframe(
    df: pd.DataFrame,
    prefix: str,
    path: str,
    bucket_name: str,
    s3_client: Any
) -> None:
    """Process and write DataFrame to S3."""
    try:
        file_name = generate_file_name(prefix, 'csv')
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"{path}{file_name}",
            Body=buffer.getvalue()
        )
    except Exception as e:
        print(f"Error writing {prefix} data: {str(e)}")
        raise

def move_processed_files(
    spotify_keys: List[str],
    bucket_name: str,
    s3_resource: Any
) -> None:
    """Move processed files to processed directory."""
    try:
        for key in spotify_keys:
            copy_source = {
                'Bucket': bucket_name,
                'Key': key
            }
            # Copy to processed folder
            s3_resource.meta.client.copy(
                copy_source,
                bucket_name,
                f"{PROCESSED_DATA_PATH}{key.split('/')[-1]}"
            )
            # Delete from original location
            s3_resource.Object(bucket_name, key).delete()
    except Exception as e:
        print(f"Error moving processed files: {str(e)}")
        raise

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler function."""
    try:
        # Initialize AWS clients
        s3_client = boto3.client('s3')
        s3_resource = boto3.resource('s3')
        
        # Get bucket name from environment variables
        bucket_name = os.environ.get('BUCKET_NAME')
        if not bucket_name:
            raise ValueError("Bucket name not configured in environment variables")

        # Initialize data containers
        spotify_data = []
        spotify_keys = []

        # List and process files
        try:
            response = s3_client.list_objects(Bucket=bucket_name, Prefix=RAW_DATA_PATH)
            if 'Contents' not in response:
                return {
                    'statusCode': 200,
                    'body': 'No files to process'
                }

            # Collect all JSON files
            for file in response['Contents']:
                file_key = file['Key']
                if file_key.endswith('.json'):
                    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                    content = response['Body']
                    json_data = json.loads(content.read())
                    spotify_data.append(json_data)
                    spotify_keys.append(file_key)

        except Exception as e:
            print(f"Error listing/reading S3 objects: {str(e)}")
            raise

        # Process each data file
        for data in spotify_data:
            # Extract data
            album_list = album(data)
            artist_list = artist(data)
            song_list = songs(data)

            # Create DataFrames
            album_df = pd.DataFrame.from_dict(album_list)
            artist_df = pd.DataFrame.from_dict(artist_list)
            songs_df = pd.DataFrame.from_dict(song_list)

            # Apply transformations
            album_df = album_df.drop_duplicates(subset=['album_id'])
            artist_df = artist_df.drop_duplicates(subset=['artist_id'])
            
            # Convert timestamps
            album_df['release_date'] = pd.to_datetime(album_df['release_date'], format='mixed', utc=True)
            songs_df['song_added'] = pd.to_datetime(songs_df['song_added'], format='mixed', utc=True)

            # Write transformed data
            process_and_write_dataframe(songs_df, 'songs', TRANSFORMED_PATHS['songs'], bucket_name, s3_client)
            process_and_write_dataframe(album_df, 'albums', TRANSFORMED_PATHS['albums'], bucket_name, s3_client)
            process_and_write_dataframe(artist_df, 'artists', TRANSFORMED_PATHS['artists'], bucket_name, s3_client)

        # Move processed files
        move_processed_files(spotify_keys, bucket_name, s3_resource)

        return {
            'statusCode': 200,
            'body': f'Successfully processed {len(spotify_keys)} files'
        }

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error processing data: {str(e)}'
        }
