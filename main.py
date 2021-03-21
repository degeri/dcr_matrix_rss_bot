import time

import conf
from log import logger
import matrix
import reddit


def process():
    mod_actions = reddit.new_mod_actions()
    for ma in mod_actions:
        md = reddit.format_mod_action_md(ma)
        html = reddit.format_mod_action_html(ma)
        matrix.send_message(md, html)


def main_loop():
    config = conf.config
    wait_time = int(config["programconfig"]["checktimemins"]) * 60
    mode = config["redditmodlog"]["mode"]
    logger.info("starting in '{}' mode, checking every {} seconds".format(
                    mode, wait_time))
    if mode == "json" and conf.enabled(config["redditmodlog"]["json_save_raw"]):
        logger.info("saving of raw JSON is enabled")

    while True:
        try:
            process()
        except Exception as e:
            logger.exception(e)
        time.sleep(wait_time)


def main():
    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("shutting down")


if __name__ == "__main__":
    main()
