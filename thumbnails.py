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
    if url is None:
        print("No thumbnail URL")
        return np.array([])

    try:
        async with session.get(url) as response:
            image = Image.open(BytesIO(await response.read()))
            if image.size != (480, 360):
                image = image.resize((480, 360))

            a = np.array(image)
            return a

    except Exception as e:
        print(f"Error downloading image: {e}")
        return np.array([])  # Return an empty array in case of error

async def add_thumbnail(session, video_data):
    videos = []
    tasks = []

    for item in video_data.get('items', []):
        thumbs = item['snippet']['thumbnails']

        if 'high' in thumbs:
            thumbnail_url = thumbs['high']['url'].replace('https://', 'http://')
        elif 'maxres' in thumbs:
            thumbnail_url = thumbs['maxres']['url'].replace('https://', 'http://')
        elif 'medium' in thumbs:
            thumbnail_url = thumbs['medium']['url'].replace('https://', 'http://')
        elif 'standard' in thumbs:
            thumbnail_url = thumbs['standard']['url'].replace('https://', 'http://')
        elif 'default' in thumbs:
            thumbnail_url = thumbs['default']['url'].replace('https://', 'http://')
        elif len(thumbs) > 0:
            thumbnail_url = thumbs[0]['url'].replace('https://', 'http://')
        else:
            thumbnail_url = None

        tasks.append(download_thumbnail(session, thumbnail_url))
    
    thumbnails = await asyncio.gather(*tasks)
    
    for i, item in enumerate(video_data.get('items', [])):
        if thumbnails[i].size > 0:
            videos.append({
                'videoId': item['id'], 
                'title': item['snippet']['title'],                                  # numeric
                'tags': ', '.join(item['snippet'].get('tags', [])),                 # string
                'viewCount': int(item['statistics']['viewCount']) if 'viewCount' in item['statistics'] else -1,
                'likeCount': int(item['statistics']['likeCount']) if 'likeCount' in item['statistics'] else -1,
                'commentCount': int(item['statistics']['commentCount']) if 'commentCount' in item['statistics'] else -1,
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
    
    yt = build(API_SERVICE_NAME, API_VERSION, developerKey=credentials['YOUTUBE_DATA_V3'], num_retries=5)

    outer_df = pd.DataFrame()
    connector = aiohttp.TCPConnector(limit=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        for video_ids in video_id_lists:

            video_details = get_video_details(yt, video_ids)
            videos_list = await add_thumbnail(session, video_details)

            df = pd.DataFrame(videos_list)

            if 'duration' in df.columns:
                df['duration'] = df['duration'].apply(lambda x: pd.to_timedelta(x).total_seconds())
            else:
                df['duration'] = -1

            if 'publishedAt' in df.columns:
                df['publishedAt'] = pd.to_datetime(df['publishedAt'])
                df['publishedAt'] = df['publishedAt'].apply(lambda x: x.timestamp())
            else:
                df['publishedAt'] = -1

            outer_df = pd.concat([outer_df, df], ignore_index=True)
    
    numeric_tensor = torch.empty((len(outer_df), len(config['numeric_keys'])), dtype=torch.float32)
    string_json = {}
    thumbnail_tensor = torch.empty((len(outer_df), 360, 480, 3), dtype=torch.uint8)

    for i, key in enumerate(config['numeric_keys']):
        numeric_tensor[:, i] = torch.tensor(outer_df[key].values, dtype=torch.float32)
    
    for key in config['string_keys']:
        vals = outer_df[key].values

        if isinstance(vals, np.ndarray):
            vals = vals.tolist()

        string_json[key] = vals
    
    for i, thumbnail in enumerate(outer_df['thumbnailStandard']):
        thumbnail_tensor[i] = torch.tensor(thumbnail, dtype=torch.uint8)

    numeric_filename, string_filename, thumbnail_filename = data_filenames(file_prefix)

    torch.save(numeric_tensor, numeric_filename)
    with open(string_filename, 'w') as f:
        json.dump(string_json, f)
    torch.save(thumbnail_tensor, thumbnail_filename)


def save_to_file(s3_client, bucket_name, config, 
                 video_ids, file_prefix, s3_files, 
                 remove_local=False
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

    request = yt.channels().list(
        part="contentDetails",
        id=channel_id
    )
    response = request.execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    video_ids = []
    next_page_token = None
    while True:
        playlist_request = yt.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,  # Can be adjusted up to 50
            pageToken=next_page_token
        )
        playlist_response = playlist_request.execute()
        
        video_ids.extend([item['contentDetails']['videoId'] for item in playlist_response['items']])
        
        next_page_token = playlist_response.get('nextPageToken')
        if not next_page_token:
            break

    return video_ids


if __name__ == "__main__":

    video_ids = ['1RvBnf-2YzQ', 'Xobpt0Cxmi8', 'lFwnU-oDe20']  # Replace with actual video IDs
    h5_filename = 'video_info.h5'
    asyncio.run(save_video_info([video_ids, ['-21ocG8edAY', '-40uOVBYUms', 'owlxi0VYqzM']], h5_filename))
