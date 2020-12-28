import time

from conf import config
import matrix
import reddit


wait_time = int(config["programconfig"]["checktimemins"]) * 60

while True:
    records = reddit.reddit_mod_log()
    for r in records:
        matrix.send_message(r)
    time.sleep(wait_time)
