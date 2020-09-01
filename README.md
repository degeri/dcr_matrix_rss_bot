# Decred Matrix RSS BOT

A simple bot to post rss content to a matrix room.

## Installation 

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
url : The rss feed URL
dbname : name of local sqlite database used to store feed data.
```
