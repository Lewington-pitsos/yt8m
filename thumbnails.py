from googleapiclient.discovery import build
import pandas as pd
import numpy as np
from PIL import Image
import requests
from io import BytesIO
import json
import h5py  # Import the h5py library

# Function to get YouTube video details
def get_video_details(youtube, video_ids):
    # Make the API call
    response = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=','.join(video_ids)
    ).execute()
    
    return response

def download_thumbnail(url):
    try:
        response = requests.get(url)
        image = Image.open(BytesIO(response.content))
        return np.array(image)
    except Exception as e:
        print(f"Error downloading image: {e}")
        return np.array([])  # Return an empty array in case of error

def extract_data(video_data):
    videos = []
    for item in video_data.get('items', []):
        thumbnail_url = item['snippet']['thumbnails']['standard']['url']
        thumbnail_array = download_thumbnail(thumbnail_url)

        videos.append({
            'videoId': item['id'],
            'title': item['snippet']['title'],
            'tags': ', '.join(item['snippet'].get('tags', [])),
            'viewCount': item['statistics']['viewCount'],
            'likeCount': item['statistics']['likeCount'],
            'favoriteCount': item['statistics']['favoriteCount'],
            'commentCount': item['statistics']['commentCount'],
            'duration': item['contentDetails']['duration'],  # ISO 8601 duration
            'description': item['snippet']['description'],
            'channelId': item['snippet']['channelId'],
            'channelTitle': item['snippet']['channelTitle'],
            'publishedAt': item['snippet']['publishedAt'],
            'thumbnailStandard': thumbnail_array
        })
    return videos

def main():
    with open('.credentials.json') as f:
        credentials = json.load(f)  

    API_SERVICE_NAME = 'youtube'
    API_VERSION = 'v3'
    
    video_ids = ['1RvBnf-2YzQ', 'Xobpt0Cxmi8', 'lFwnU-oDe20']  # Replace with actual video IDs
    
    youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=credentials['YOUTUBE_DATA_V3'])
    video_details = get_video_details(youtube, video_ids)
    
    videos_list = extract_data(video_details)
    df = pd.DataFrame(videos_list)
    
    # Convert ISO 8601 duration to seconds
    df['duration_seconds'] = df['duration'].apply(lambda x: pd.to_timedelta(x).total_seconds())
    
    # Drop the original duration column
    df.drop('duration', axis=1, inplace=True)
    
    # Save to HDF5 file
    with h5py.File('youtube_video_details.hdf5', 'w') as hf:
        for column in df.columns:
            if df[column].dtype == object and column != 'thumbnailStandard':  # Handle string data
                encoded_data = df[column].apply(lambda x: x.encode('utf-8') if isinstance(x, str) else x)
                hf.create_dataset(column, (len(encoded_data),), dtype=h5py.special_dtype(vlen=str), data=encoded_data)
            elif column == 'thumbnailStandard':
                # Ensure all thumbnail arrays are the same shape
                max_shape = max((img.shape for img in df[column]), key=lambda x: (x[0], x[1]))
                # Pad images to have the same max shape
                thumbnail_data = np.array([np.pad(img, [(0, max_shape[0] - img.shape[0]), (0, max_shape[1] - img.shape[1]), (0, 0)], mode='constant') for img in df[column]])
                hf.create_dataset(column, data=thumbnail_data, dtype=np.uint8)
            else:
                hf.create_dataset(column, data=df[column])

if __name__ == "__main__":
    main()
