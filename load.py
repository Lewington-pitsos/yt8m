import torch
import h5py
from torch.utils.data import Dataset

class VideoDataDataset(Dataset):
    def __init__(self, file_paths):
        self.file_paths = file_paths  # List of HDF5 file paths
        self.index_map = []
        for file_idx, file_path in enumerate(self.file_paths):
            with h5py.File(file_path, 'r') as hf:
                keys = list(hf.keys())
                # Assuming all datasets have the same length for simplicity
                num_samples = len(hf[keys[0]])
                for i in range(num_samples):
                    self.index_map.append((file_idx, i))

    def __len__(self):
        return len(self.index_map)

    def __getitem__(self, idx):
        file_idx, local_idx = self.index_map[idx]
        file_path = self.file_paths[file_idx]
        with h5py.File(file_path, 'r') as hf:
            keys = list(hf.keys())
            data = {
                'thumbnailStandard': torch.tensor(hf['thumbnailStandard'][local_idx]),
                'likeCount': hf['likeCount'][local_idx] if 'likeCount' in keys else None,
                'viewCount': hf['viewCount'][local_idx] if 'viewCount' in keys else None,
                'commentCount': hf['commentCount'][local_idx] if 'commentCount' in keys else None,
                'title': hf['title'][local_idx].decode('utf-8') if 'title' in keys else None
            }
        return data

# Usage
file_paths = ['../yt8m/video/video_ids.train0093.h5']  # Add your file paths
dataset = VideoDataDataset(file_paths)

# Example access
img = dataset[10]['thumbnailStandard']  # Access the 10th sample's thumbnail
