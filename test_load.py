from load import VideoDataset

def test_load():
    ds = VideoDataset('test_data/mrbeast')

    sample = ds[23]

    assert 'viewCount' in sample
    assert 'likeCount' in sample
    assert 'commentCount' in sample
    assert 'publishedAt' in sample
    assert 'thumbnail' in sample
    assert 'videoId' in sample
    assert 'title' in sample

    print(sample)