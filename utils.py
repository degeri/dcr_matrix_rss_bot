import json
import time

from log import logger


def json_compact(obj):
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def request_retrying(fn, retries=0, wait_sec=1):
    retry = 0
    while retry <= retries:
        resp = fn()
        if resp.status_code == 200:
            return resp
        else:
            logger.warning("response status {}, retrying in {} s".format(
                str(resp.status_code), wait_sec))
            retry += 1
            time.sleep(wait_sec)
    logger.warning("failed to fetch with {} retries, giving up this"
                   " request".format(retries))
    return None
