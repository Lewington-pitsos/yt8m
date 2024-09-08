import h5py
import torch
from torch.utils.data import IterableDataset

class VideoDataIterableDataset(IterableDataset):
    def __init__(self, file_paths):
        super(VideoDataIterableDataset, self).__init__()
        self.file_paths = file_paths  # List of HDF5 file paths

    def read_hdf5(self, file_path):
        with h5py.File(file_path, 'r') as hf:
            keys = list(hf.keys())

            for i in range(len(hf[keys[0]])):  # Assuming all datasets have the same length
                thumbnail = torch.tensor(hf['thumbnailStandard'][i])

                yield {
                    'thumbnailStandard': thumbnail,
                    'likeCount': hf['likeCount'][i] if 'likeCount' in keys else None,
                    'viewCount': hf['viewCount'][i] if 'viewCount' in keys else None,
                    'commentCount': hf['commentCount'][i] if 'commentCount' in keys else None,
                    'title': hf['title'][i].decode('utf-8') if 'title' in keys else None
                }

    def __iter__(self):
        for file_path in self.file_paths:
            yield from self.read_hdf5(file_path)

# Usage
file_paths = ['video/video_ids.train3529.h5']  # Add your file paths
dataset = VideoDataIterableDataset(file_paths)

for data in dataset:
    print(data)
