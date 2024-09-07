import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import time
import tensorflow as tf
import aiohttp
import asyncio
import multiprocessing
tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR) 



async def resolve_video_id(session, random_id):
    url_prefix = "http://data.yt8m.org/2/j/i/"
    url_suffix = f"{random_id[:2]}/{random_id}.js"
    full_url = url_prefix + url_suffix
    
    try:
        async with session.get(full_url) as response:
            if response.status == 200:
                content = await response.text()
                start = content.find('"') + 1
                start = content.find('"', start) + 1
                start = content.find('"', start) + 1
                end = content.rfind('"')
                return content[start:end]
            else:
                pass
    except Exception as e:
        print(f"Error while requesting {full_url}: {str(e)}")
    return None

def _parse_function(proto):
    keys_to_features = {
        'id': tf.io.FixedLenFeature([], tf.string),
        'labels': tf.io.VarLenFeature(tf.int64),
    }
    return tf.io.parse_single_example(proto, keys_to_features)

async def resolve_ids(dataset):
    async with aiohttp.ClientSession() as session:
        tasks = [resolve_video_id(session, record['id'].numpy().decode('utf-8')) for record in dataset]
        resolved_ids = await asyncio.gather(*tasks)
        return resolved_ids

def serialize_example(original_example, video_id):
    labels_dense = tf.sparse.to_dense(original_example['labels'])
    
    feature = {
        'id': tf.train.Feature(bytes_list=tf.train.BytesList(value=[original_example['id'].numpy()])),
        'labels': tf.train.Feature(int64_list=tf.train.Int64List(value=labels_dense.numpy())),
        'video_id': tf.train.Feature(bytes_list=tf.train.BytesList(value=[bytes(video_id, 'utf-8')]))
    }
    example_proto = tf.train.Example(features=tf.train.Features(feature=feature))
    return example_proto.SerializeToString()

async def update_tfrecord(input_path, output_path):
    dataset = tf.data.TFRecordDataset(input_path)
    dataset = dataset.map(_parse_function)
    resolved_ids = await resolve_ids(dataset)
    
    with tf.io.TFRecordWriter(output_path) as writer:
        count = 0
        for record, video_id in zip(dataset, resolved_ids):
            if video_id is not None:
                example = serialize_example(record, video_id)
                writer.write(example)
                count += 1

        print(f"Processed {count} records from {input_path} out of {len(resolved_ids)}, percentage: {count / len(resolved_ids) * 100}%")

def process_file(input_path, output_path):
    start = time.time()
    asyncio.run(update_tfrecord(input_path, output_path))
    print(f"Processed {input_path} in {time.time() - start} seconds")

if __name__ == '__main__':
    files = ['video/' + f for f in os.listdir('video') if f.endswith('.tfrecord') and 'video_ids' not in f]
    files = files
    output_paths = [f.replace('train', 'video_ids.train') for f in files]

    print(files)

    start = time.time()
    with multiprocessing.Pool(processes=12) as pool:
        pool.starmap(process_file, zip(files, output_paths))
    
    print(f"Total execution time: {time.time() - start} seconds")
