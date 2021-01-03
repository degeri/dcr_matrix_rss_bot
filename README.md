# Decred Matrix Atom BOT

A simple bot to post Atom content to a matrix room.

## Installation 

Copy `config.ini.sample` into `config.ini` and edit the latter as needed. `config.ini` is ignored by Git to prevent from accidental leaking of secrets to the repository.

Edit the ini file and simply run

```
python3 main.py
```

For continuous monitoring keep the terminal open or add it as a service. 

## How to edit the config.ini file


programconfig

```
checktimemins : How mins to wait before doing an new check
```


matrixconfig

```
accesstoken  : Your private access token for the bot

example curl to obtain:

curl -XPOST -d '{"type":"m.login.password", "user":"USER_REPLACE", "password":"PASSWORD_REPLACE"}' "https://matrix.decred.org/_matrix/client/r0/login"

roomid: Internal room ID eg:!XXXXXXXXXXXXXXX:decred.org

server_url : Matrix Server URL eg: https://matrix.decred.org/


```

redditmodlog

```
atom_url : The Atom feed URL
json_url : The JSON feed URL
mode : "atom" or "json"
dbfile : name of local sqlite database used to store feed data.
json_save_raw : true to enable saving raw modlog JSON objects, only supported in json mode
json_raw_dbfile : name of local sqlite database for raw mod log data
```

Currently only a subset of Reddit response is used and saved in `dbfile`. When `json_save_raw` is `true`, raw JSON mod action objects (unmodified and unfiltered) are stored in a separate database file. If in the future we decide to extract and use more parts of Reddit modlog response, it will be useful to have full raw past data available. Note that Reddit allows to get the mod log only ~2 months into the past.

To obtain Reddit mod log feed URLs:

- open Reddit [preferences](https://www.reddit.com/prefs/)
- check `enable private RSS feeds`
- open [RSS feeds](https://www.reddit.com/prefs/feeds/) tab
- copy the `RSS` (it is Atom really) or `JSON` link for `moderation log`
- **treat the link as secret and do not share it**
- change `r/mod` to **your subreddit** in the link

The result should look like this:

    https://www.reddit.com/r/MyGreatSubreddit/about/log/.rss?feed=xxxxxxxxx&user=xxxxxxxx

If you _really_ want to you can use `r/mod` but mind that it will show mod activity from **all subreddits** you moderate.

We may add support for multiple explicitly set subreddits if necessary.
