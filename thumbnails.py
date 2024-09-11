import os
import aiohttp
import asyncio
from googleapiclient.discovery import build
import pandas as pd
import numpy as np
from PIL import Image
from io import BytesIO
import json
import torch

YT_DATALIST_LIMIT = 50
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

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
            if image.size != (480, 360):
                return np.array([])  # Return an empty array if the image is not the correct size

            a = np.array(image)
            if a.shape != (360, 480, 3):
                return np.array([])
            return a

    except Exception as e:
        print(f"Error downloading image: {e}")
        return np.array([])  # Return an empty array in case of error

async def add_thumbnail(session, video_data):
    videos = []
    tasks = []

    for item in video_data.get('items', []):
        thumbnail_url = item['snippet']['thumbnails']['high']['url'].replace('https://', 'http://')
        tasks.append(download_thumbnail(session, thumbnail_url))
    
    thumbnails = await asyncio.gather(*tasks)
    
    for i, item in enumerate(video_data.get('items', [])):
        if 'viewCount' in item['statistics'] and 'likeCount' in item['statistics'] and 'commentCount' in item['statistics'] and thumbnails[i].size > 0:
            videos.append({
                'videoId': item['id'], 
                'title': item['snippet']['title'],                                  # numeric
                'tags': ', '.join(item['snippet'].get('tags', [])),                 # string
                'viewCount': int(item['statistics']['viewCount']),                  # numeric
                'likeCount': int(item['statistics']['likeCount']),                  # numeric
                'commentCount': int(item['statistics']['commentCount']),            # numeric
                'duration': item['contentDetails']['duration'],                     # numeric
                'description': item['snippet']['description'],                      # string
                'channelId': item['snippet']['channelId'],                          # string
                'channelTitle': item['snippet']['channelTitle'],                    # string
                'publishedAt': item['snippet']['publishedAt'],                      # numeric
                'thumbnailStandard': thumbnails[i]                                  # image
            })
    return videos


def data_filenames(prefix):
    return f"{prefix}-numeric.pt", f"{prefix}-string.json", f"{prefix}-thumbnail.pt"

async def save_video_info(config, video_id_lists, file_prefix):
    with open('.credentials.json') as f:
        credentials = json.load(f)  


    
    youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=credentials['YOUTUBE_DATA_V3'], num_retries=5)

    outer_df = pd.DataFrame()
    connector = aiohttp.TCPConnector(limit=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        for video_ids in video_id_lists:

            video_details = get_video_details(youtube, video_ids)
            videos_list = await add_thumbnail(session, video_details)

            df = pd.DataFrame(videos_list)

            if 'duration' in df.columns:
                df['duration_seconds'] = df['duration'].apply(lambda x: pd.to_timedelta(x).total_seconds())
                df.drop('duration', axis=1, inplace=True)

                outer_df = pd.concat([outer_df, df], ignore_index=True)
            else:
                print("Error: No duration column in DataFrame", df)
    
    numeric_tensor = torch.empty((len(outer_df), len(config['numeric_keys'])), dtype=torch.float32)
    string_json = {}
    thumbnail_tensor = torch.empty((len(outer_df), 360, 480, 3), dtype=torch.uint8)

    for i, key in enumerate(config['numeric_keys']):
        numeric_tensor[:, i] = torch.tensor(outer_df[key].values, dtype=torch.float32)
    
    for key in config['string_keys']:
        string_json[key] = outer_df[key].values
    
    for i, thumbnail in enumerate(outer_df['thumbnailStandard']):
        thumbnail_tensor[i] = torch.tensor(thumbnail, dtype=torch.uint8)

    numeric_filename, string_filename, thumbnail_filename = data_filenames(file_prefix)

    torch.save(numeric_tensor, numeric_filename)
    with open(string_filename, 'w') as f:
        json.dump(string_json, f)
    torch.save(thumbnail_tensor, thumbnail_filename)


def save_to_file(s3_client, bucket_name, config, 
                 video_ids, file_prefix, 
                 s3_files, remove_local=False
    ):
    
    numeric_file, string_file, thumbnail_file = data_filenames(file_prefix)
    if os.path.exists(numeric_file) and os.path.exists(string_file) and os.path.exists(thumbnail_file):
        print(f"Skipping {file_prefix} because {numeric_file}, and {string_file} and {thumbnail_file} already exist")
        return

    file_keys = [f['Key'] for f in s3_files]
    if numeric_file in file_keys and string_file in file_keys and thumbnail_file in file_keys:
        print(f"Skipping {file_prefix} because {file_prefix} files already exist in s3")
        return


    batches = []
    for i in range(0, len(video_ids), YT_DATALIST_LIMIT):
        batches.append(video_ids[i:i + YT_DATALIST_LIMIT])

    asyncio.run(save_video_info(config, batches, file_prefix))

    for path in [numeric_file, string_file, thumbnail_file]:
        s3_client.upload_file(path, bucket_name, path)
        if remove_local:
            os.remove(path)


def get_video_ids(channel_id):
    with open('.credentials.json') as f:
        credentials = json.load(f)

    yt = build(API_SERVICE_NAME, API_VERSION, developerKey=credentials['YOUTUBE_DATA_V3'], num_retries=5)

    pass



if __name__ == "__main__":

    video_ids = ['1RvBnf-2YzQ', 'Xobpt0Cxmi8', 'lFwnU-oDe20']  # Replace with actual video IDs
    h5_filename = 'video_info.h5'
    asyncio.run(save_video_info([video_ids, ['-21ocG8edAY', '-40uOVBYUms', 'owlxi0VYqzM']], h5_filename))
