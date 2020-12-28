import time

from conf import config
import matrix
import reddit


wait_time = int(config["programconfig"]["checktimemins"]) * 60

while True:
    records = reddit.new_modlog_records()
    for r in records:
        matrix.send_message(r)
    time.sleep(wait_time)
