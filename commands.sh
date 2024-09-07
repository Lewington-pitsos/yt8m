curl data.yt8m.org/download.py | shard=1,100 partition=2/frame/train mirror=asia python


curl data.yt8m.org/download.py | shard=1,100 partition=2/video/train mirror=asia python



curl data.yt8m.org/download.py | partition=2/video/train mirror=asia python &&\
curl data.yt8m.org/download.py | partition=2/video/validate mirror=asia python &&\
curl data.yt8m.org/download.py | partition=2/video/test mirror=asia python