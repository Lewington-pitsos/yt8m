import os
import boto3
import json
import time
from thumbnails import save_to_file, get_video_ids

channel_mapping = {
    'UCX6OQ3DkcsbYNE6H8uQQuVA': 'mrbeast',
}


if __name__ == '__main__':
    channel_id = 'UCX6OQ3DkcsbYNE6H8uQQuVA'
    channel_name = channel_mapping[channel_id]
    bucket_name = 'vit-sae'
    max_videos_per_file = 1000


    config = {
        'numeric_keys': ['viewCount', 'likeCount', 'commentCount', 'publishedAt', 'duration'],
        'string_keys': ['videoId', 'title', 'tags', 'description', 'channelId', 'channelTitle'],
    }

    with open('.credentials.json') as f:
        credentials = json.load(f)

    s3_client = boto3.client('s3',aws_access_key_id=credentials['AWS_ACCESS_KEY_ID'], aws_secret_access_key=credentials['AWS_SECRET'])
    s3_client.put_object(Bucket=bucket_name, Key=f'{channel_name}/config.json', Body=json.dumps(config))
    s3_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f'{channel_name}/')['Contents']

    all_video_ids = get_video_ids(channel_id)

    print(f"Found {len(all_video_ids)} videos for channel {channel_id}")

    if not os.path.exists(channel_name):
        os.makedirs(channel_name)
        
    for i in range(0, len(all_video_ids), max_videos_per_file):
        vid_id_batch = all_video_ids[i:i + max_videos_per_file]
        file_prefix = f"{channel_name}/{i}"
        
        save_to_file(s3_client, bucket_name, config, vid_id_batch, file_prefix, s3_files, remove_local=False)