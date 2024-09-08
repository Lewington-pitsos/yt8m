import aiohttp
import asyncio
from googleapiclient.discovery import build
import pandas as pd
import numpy as np
from PIL import Image
import requests
from io import BytesIO
import json
import h5py  # Import the h5py library

def get_video_details(youtube, video_ids):
    # Make the API call
    response = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=','.join(video_ids)
    ).execute()
    
    return response

async def download_thumbnail(session, url):
    try:
        async with session.get(url) as response:
            image = Image.open(BytesIO(await response.read()))
            return np.array(image)

    except Exception as e:
        print(f"Error downloading image: {e}")
        return np.array([])  # Return an empty array in case of error

async def extract_data(session, video_data):
    videos = []
    tasks = []

    for item in video_data.get('items', []):
        thumbnail_url = item['snippet']['thumbnails']['high']['url'].replace('https://', 'http://')
        tasks.append(download_thumbnail(session, thumbnail_url))
    
    thumbnails = await asyncio.gather(*tasks)
    
    for i, item in enumerate(video_data.get('items', [])):
        if 'viewCount' in item['statistics'] and 'likeCount' in item['statistics'] and 'commentCount' in item['statistics']:
            videos.append({
                'videoId': item['id'],
                'title': item['snippet']['title'],
                'tags': ', '.join(item['snippet'].get('tags', [])),
                'viewCount': int(item['statistics']['viewCount']),
                'likeCount': int(item['statistics']['likeCount']),
                'commentCount': int(item['statistics']['commentCount']),
                'duration': item['contentDetails']['duration'],
                'description': item['snippet']['description'],
                'channelId': item['snippet']['channelId'],
                'channelTitle': item['snippet']['channelTitle'],
                'publishedAt': item['snippet']['publishedAt'],
                'thumbnailStandard': thumbnails[i]
            })
    return videos

async def save_video_info(video_id_lists, h5_filename):
    with open('.credentials.json') as f:
        credentials = json.load(f)  

    API_SERVICE_NAME = 'youtube'
    API_VERSION = 'v3'
    
    youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=credentials['YOUTUBE_DATA_V3'], num_retries=5)

    outer_df = pd.DataFrame()
    connector = aiohttp.TCPConnector(limit=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        for video_ids in video_id_lists:

            video_details = get_video_details(youtube, video_ids)
    
            videos_list = await extract_data(session, video_details)
            df = pd.DataFrame(videos_list)

            df['duration_seconds'] = df['duration'].apply(lambda x: pd.to_timedelta(x).total_seconds())
            df.drop('duration', axis=1, inplace=True)

            outer_df = pd.concat([outer_df, df], ignore_index=True)
            
    with h5py.File(h5_filename, 'w') as hf:
        for column in outer_df.columns:
            if outer_df[column].dtype == object and column != 'thumbnailStandard':  # Handle string data
                encoded_data = outer_df[column].apply(lambda x: x.encode('utf-8') if isinstance(x, str) else x)
                hf.create_dataset(column, (len(encoded_data),), dtype=h5py.special_dtype(vlen=str), data=encoded_data)
            elif column == 'thumbnailStandard':
                # Ensure all thumbnail arrays are the same shape
                max_shape = max((img.shape for img in outer_df[column]), key=lambda x: (x[0], x[1]))
                # Pad images to have the same max shape
                thumbnail_data = np.array([np.pad(img, [(0, max_shape[0] - img.shape[0]), (0, max_shape[1] - img.shape[1]), (0, 0)], mode='constant') for img in df[column]])
                hf.create_dataset(column, data=thumbnail_data, dtype=np.uint8)
            else:
                hf.create_dataset(column, data=outer_df[column])

if __name__ == "__main__":

    video_ids = ['1RvBnf-2YzQ', 'Xobpt0Cxmi8', 'lFwnU-oDe20']  # Replace with actual video IDs
    h5_filename = 'video_info.h5'
    asyncio.run(save_video_info([video_ids, ['-21ocG8edAY', '-40uOVBYUms', 'owlxi0VYqzM']], h5_filename))
