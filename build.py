import json
import boto3
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import asyncio
import time
from thumbnails import save_to_file
from resolve_ids import resolve_all
import tensorflow as tf
tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR) 

def _parse_function(proto):
    keys_to_features = {
        'id': tf.io.FixedLenFeature([], tf.string),
        'labels': tf.io.VarLenFeature(tf.int64),
        'video_id': tf.io.FixedLenFeature([], tf.string)
    }
    return tf.io.parse_single_example(proto, keys_to_features)


def main(bucket_name, input_path, remove_local_h5=False):
    start = time.time()

    config = {
        'numeric_keys': ['viewCount', 'likeCount', 'commentCount', 'publishedAt', 'duration', ],
        'string_keys': ['videoId', 'title', 'tags', 'description', 'channelId', 'channelTitle'],
    }

    with open('.credentials.json') as f:
        credentials = json.load(f)

    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=credentials['AWS_SECRET']
    )
    tfrecords = [input_path +'/' + f for f in os.listdir('video') if f.endswith('.tfrecord') and 'video_ids' not in f]

    print(f"Found {len(tfrecords)} tfrecords")

    id_files = resolve_all(tfrecords)

    end = time.time()
    print(f"Resolved all ids in {end - start} seconds")

    s3_client.put_object(Bucket=bucket_name, Key=f'{input_path}/config.json', Body=json.dumps(config))
    s3_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f'{input_path}/')['Contents']

    for id_file in id_files:
        start = time.time()
        dataset = tf.data.TFRecordDataset(id_file)
        dataset = dataset.map(_parse_function)

        video_ids = []
        for record in dataset:
            video_id = record['video_id'].numpy().decode('utf-8')
            video_ids.append(video_id)

        file_prefix = f"{id_file.replace('.tfrecord', '')}"
        save_to_file(s3_client, bucket_name, config, video_ids, file_prefix, s3_files, remove_local_h5)

        end = time.time()
        print(f"Processed {id_file} in {end - start} seconds")

if __name__ == '__main__':
    s3_prefix = 'yt8m-thumbs'
    main('vit-sae', 'video', s3_prefix, remove_local_h5=True)