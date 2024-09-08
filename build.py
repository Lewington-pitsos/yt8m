import json
import boto3
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import asyncio
import time
from thumbnails import save_video_info
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


YT_DATALIST_LIMIT = 50

def main(bucket_name):
    start = time.time()

    with open('.credentials.json') as f:
        credentials = json.load(f)

    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=credentials['AWS_SECRET']
    )

    s3_prefix = 'yt8m-thumbs'
    tfrecords = ['video/' + f for f in os.listdir('video') if f.endswith('.tfrecord') and 'video_ids' not in f]

    print(f"Found {len(tfrecords)} tfrecords")

    id_files = resolve_all(tfrecords)

    end = time.time()
    print(f"Resolved all ids in {end - start} seconds")

    for id_file in id_files:
        start = time.time()
        h5_filename = f"{id_file.replace('.tfrecord', '.h5')}"
        if os.path.exists(h5_filename):
            print(f"Skipping {id_file} because {h5_filename} already exists")
            continue


        dataset = tf.data.TFRecordDataset(id_file)
        dataset = dataset.map(_parse_function)
        
        video_ids = []
        for record in dataset:
            video_id = record['video_id'].numpy().decode('utf-8')
            video_ids.append(video_id)

        batches = []
        for i in range(0, len(video_ids), YT_DATALIST_LIMIT):
            batches.append(video_ids[i:i + YT_DATALIST_LIMIT])
        
        asyncio.run(save_video_info(batches, h5_filename))
        s3_client.upload_file(h5_filename, bucket_name, f'{s3_prefix}/{h5_filename}')
        end = time.time()
        print(f"Processed {id_file} in {end - start} seconds")

if __name__ == '__main__':
    main('vit-sae')