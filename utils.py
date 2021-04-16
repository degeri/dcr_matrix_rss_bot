import json
import time
from urllib.parse import urlparse

from log import logger


def json_compact(obj):
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def request_retrying(fn, url, retries=0, wait_sec=1):
    retry = 0
    host = urlparse(url).hostname
    while retry <= retries:
        resp = fn(url)
        if resp.status_code == 200:
            return resp
        else:
            logger.warning("{}: response status {}, retrying in {} s".format(
                host, str(resp.status_code), wait_sec))
            retry += 1
            time.sleep(wait_sec)
    logger.warning("{}: failed to fetch with {} retries, giving up this"
                   " request".format(host, retries))
    return None
