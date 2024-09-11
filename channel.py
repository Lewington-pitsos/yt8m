import os
import boto3
import json
import time
from thumbnails import save_to_file, get_video_ids

channel_mapping = {
    'UC8butISFwT-Wl7EV0hUK0BQ': 'sentdex',
}


if __name__ == '__main__':
    channel_id = 'UC8but'
    channel_name = channel_mapping[channel_id]
    bucket_name = 'vit-sae'
    max_videos_per_file = 1000

    config = {
        'numeric_keys': ['viewCount', 'likeCount', 'commentCount', 'publishedAt', 'duration'],
        'string_keys': ['videoId', 'title', 'tags', 'description', 'channelId', 'channelTitle'],
    }
    with open('.credentials.json') as f:
        credentials = json.load(f)

    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=credentials['AWS_SECRET']
    )

    s3_client.put_object(Bucket=bucket_name, Key=f'{channel_name}/config.json', Body=json.dumps(config))
    s3_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f'{channel_name}/')['Contents']

    all_video_ids = get_video_ids(channel_id)

    vid_id_batches = []
    for i in range(0, len(all_video_ids), max_videos_per_file):
        vid_id_batches.append(all_video_ids[i:i + max_videos_per_file])

    for i, vid_ids in enumerate(vid_id_batches):
        file_prefix = f"{channel_name}/{i}"
        save_to_file(s3_client, bucket_name, config, vid_ids, file_prefix, s3_files, remove_local=False)