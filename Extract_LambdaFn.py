import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # Get credentials from environment variables
    client_id = os.environ.get("SPOTIFY_CLIENT_ID")         # Modified to be more explicit
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET") # Modified to be more explicit
    
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    # You might want to make this configurable or use environment variables
    playlist_link = 'https://open.spotify.com/playlist/YOUR_PLAYLIST_ID'  # Replace with placeholder
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    
    spotify_data = sp.playlist_tracks(playlist_URI)
    print(spotify_data)
    
    # AWS S3 upload
    client = boto3.client('s3')
    file_name = 'spotify_raw_' + str(datetime.now()) + '.json'
    client.put_object(
        Bucket='YOUR_BUCKET_NAME',  # Replace with placeholder
        Key='raw_data/to_processed/' + file_name,
        Body=json.dumps(spotify_data)
    )
