Given Lastfm dataset: http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz 
Build REST web service providing endpoints for:

1. List of all unique users.
2. List of n top users and the number of distinct songs played by each of them, sorted in  decreasing order by the number of distinct songs (i.e., the user with the highest number of distinct songs listened appearing at the top of the list).
3. List of n top most frequently listened songs and the number of times each of them was played, sorted in decreasing order by the number of times a song was played.
4. List of n top longest listening sessions*, with information on their duration, the user, and the songs listened, sorted decreasingly by session length.
5. Given a user ID, predict the next time the user will be listening to any content.)
6. Given a user ID, predict the next song(s) the user will be listening to.
7. Given a user ID, recommend songs (or artists) that the user has not listened to yet, but might want to.

*) a session is defined as one or more songs played by a particular user, where each song is started within 20 minutes of the previous song's start time.
