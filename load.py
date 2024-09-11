import json
import os
import torch
import h5py
from torch.utils.data import Dataset
from collections import defaultdict

# add device

class VideoDataset(Dataset):
    def __init__(self, path, data_types=['numeric', 'string', 'thumbnail']):
        assert len(data_types) > 0, "Data types must be specified"
        assert all([data_type in ['numeric', 'string', 'thumbnail'] for data_type in data_types]), "Data types must be one of ['numeric', 'string', 'thumbnail']"
        assert os.path.exists(path), f"Path {path} does not exist"

        self.parent_dir = path  
        config_path = os.path.join(self.parent_dir, 'config.json')
        self.data_types = data_types

        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.files = defaultdict(list)
        
        data_files = os.listdir(self.parent_dir)
        for file in data_files:
            for file_type in self.data_types:
                if file_type in file:
                    self.files[file_type].append(file)

        n_files = 0
        for k, v in self.files.items():
            if n_files == 0:
                n_files = len(v)
            else:
                assert len(v) == n_files, f"Number of {k} files, {len(v)} is not equal to number of a different file type {n_files}"

        if n_files == 0:
            raise ValueError("No files found for any of the data types", self.data_types, data_files)

        self.index_map = []
        self.lengths = []

        easy_data_type = self._get_easy_data_type()
        for i in range(n_files):
            length = self._file_len(i, easy_data_type)
            self.index_map += [i for _ in range(length)]
            self.lengths.append(length)

        
    def _file_len(self, file_idx, data_type):
        data = self._load_file(file_idx, data_type)

        if data_type == 'string':
            for v in data.values():
                return len(v)

        return data.shape[0]

    def _load_file(self, file_idx, data_type):
        file_path = os.path.join(self.parent_dir, self.files[data_type][file_idx])

        if data_type == 'string':
            with open(file_path, 'r') as f:
                return json.load(f)

        return torch.load(file_path)
    

    def _numeric_to_dict(self, data):
        data_dict = {}
        for i, key in enumerate(self.config['numeric_keys']):
            data_dict[key] = data[:, i]

        return data_dict

    def _load_data_dict(self, file_idx, data_type):
        data = self._load_file(file_idx, data_type)

        if data_type == 'string':
            return data

        if data_type == 'thumbnail':
            return {'thumbnail': data}

        if data_type == 'numeric':
            return self._numeric_to_dict(data)

    def _get_easy_data_type(self):
        if 'numeric' in self.data_types:
            return 'numeric'
        elif 'string' in self.data_types:
            return 'string'

        return 'thumbnail'
        

    def __len__(self):
        return len(self.index_map)

    def _get_slice_efficient(self, idx):
        pass

    def _get_single_item(self, idx):
        file_idx = self.index_map[idx]
        data = {}

        item_offset = idx - sum(self.lengths[:file_idx])

        for data_type in self.data_types:
            data_dict = self._load_data_dict(file_idx, data_type)

            if data_type == 'string':
                for k, v in data_dict.items():
                    data[k] = v[item_offset]
            else:
                for k, v in data_dict.items():
                    data[k] = v[item_offset]

        return data

    def __getitem__(self, idx):

        if isinstance(idx, slice):
            return self._get_slice_efficient(idx)

        return self._get_single_item(idx)