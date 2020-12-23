from functions import *
from log import *
from conf import *

wait_time = int(config["programconfig"]["checktimemins"]) * 60

while True:
    reddit_mod_log()
    time.sleep(wait_time)
