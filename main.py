import time

from conf import config
from functions import reddit_mod_log
import matrix


wait_time = int(config["programconfig"]["checktimemins"]) * 60

while True:
    records = reddit_mod_log()
    for r in records:
        matrix.send_message(r)
    time.sleep(wait_time)
