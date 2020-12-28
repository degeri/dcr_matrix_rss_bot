import time

from conf import config
from functions import reddit_mod_log


wait_time = int(config["programconfig"]["checktimemins"]) * 60

while True:
    reddit_mod_log()
    time.sleep(wait_time)
