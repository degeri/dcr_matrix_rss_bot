import time

from conf import config
import matrix
import reddit


wait_time = int(config["programconfig"]["checktimemins"]) * 60

while True:
    mod_actions = reddit.new_mod_actions()
    for ma in mod_actions:
        md = reddit.format_mod_action_md(ma)
        matrix.send_message(md)
    time.sleep(wait_time)
