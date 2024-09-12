from load import VideoDataset

def test_load():
    ds = VideoDataset('test_data/mrbeast')

    assert len(ds) == 50

    sample = ds[23]

    assert 'viewCount' in sample
    assert 'likeCount' in sample
    assert 'commentCount' in sample
    assert 'publishedAt' in sample
    assert 'thumbnail' in sample
    assert 'videoId' in sample
    assert 'title' in sample

    samples = ds[1:10]
    assert len(samples['viewCount']) == 9
    assert len(samples['likeCount']) == 9
    assert samples['thumbnail'].shape[0] == 9


    samples = ds[0:10]
    assert len(samples['viewCount']) == 10
    assert len(samples['likeCount']) == 10
    assert samples['thumbnail'].shape[0] == 10


    samples = ds[9:50]
    assert len(samples['viewCount']) == 41
    assert len(samples['likeCount']) == 41
    assert samples['thumbnail'].shape[0] == 41

    samples = ds[10:52]
    assert len(samples['viewCount']) == 40
    assert len(samples['likeCount']) == 40
    assert samples['thumbnail'].shape[0] == 40

    samples = ds[10:11]
    assert len(samples['viewCount']) == 1
    assert len(samples['likeCount']) == 1
    assert samples['thumbnail'].shape[0] == 1

    samples = ds[9:10]
    assert len(samples['viewCount']) == 1
    assert len(samples['likeCount']) == 1
    assert samples['thumbnail'].shape[0] == 1