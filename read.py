import time
import tensorflow as tf
import aiohttp
import asyncio

path = 'video/train0208.tfrecord'
output_path = 'video/video_ids.train0208.tfrecord'

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
                print(f"Failed to resolve video ID for {random_id}, HTTP Status: {response.status}")
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
        for record, video_id in zip(dataset, resolved_ids):
            if video_id is not None:
                example = serialize_example(record, video_id)
                writer.write(example)

if __name__ == '__main__':
    start = time.time()
    # Run the async function to update the TFRecord
    asyncio.run(update_tfrecord(path, output_path))

    print(f"Execution time: {time.time() - start} seconds")