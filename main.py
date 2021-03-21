import time

from conf import config
from log import logger
import matrix
import reddit


wait_time = int(config["programconfig"]["checktimemins"]) * 60


def process():
    mod_actions = reddit.new_mod_actions()
    for ma in mod_actions:
        md = reddit.format_mod_action_md(ma)
        html = reddit.format_mod_action_html(ma)
        matrix.send_message(md, html)


def main():
    while True:
        try:
            process()
        except Exception as e:
            logger.exception(e)
        time.sleep(wait_time)


if __name__ == "__main__":
    main()
