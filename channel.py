import os
import boto3
import json
import time
from thumbnails import save_to_file, get_video_ids

channel_mapping = {
    'UCX6OQ3DkcsbYNE6H8uQQuVA': 'mrbeast',
    # 'UCk8GzjMOrta8yxDcKfylJYw': 'kids-diana-show',
    # 'UCbCmjCuTUZos6Inko4u57UQ': 'cocomelon',
    # 'UCJplp5SjeGSdVdwsfb9Q7lQ': 'like-nastya',
    # 'UCvlE5gTbOvjiolFlEm-c_Ow': 'vlad-and-niki',
    # 'UCFFbwnve3yF62-tVXkTyHqg': 'zee-music-channel',
    # 'UCq-Fj5jknLsUf-MWSy4_brA': 'tseries',
}


if __name__ == '__main__':
    parent_dir = 'test_data'
    bucket_name = 'vit-sae'
    max_videos_per_file = 10

    for channel_id, channel_name in channel_mapping.items():
        print("starting with", channel_name)

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

        print(f"Found {len(all_video_ids)} videos for channel {channel_name}, {channel_id}")

        common_prefix = f"{parent_dir}/{channel_name}"
        if not os.path.exists(common_prefix):
            os.makedirs(common_prefix)

        # save config file locally
        with open(os.path.join(common_prefix, "config.json"), 'w') as f:
            json.dump(config, f)
            
        for i in range(0, len(all_video_ids), max_videos_per_file):
            vid_id_batch = all_video_ids[i:i + max_videos_per_file]
            file_prefix = f"{common_prefix}/{i}"
            
            save_to_file(s3_client, bucket_name, config, vid_id_batch, file_prefix, s3_files, remove_local=False)