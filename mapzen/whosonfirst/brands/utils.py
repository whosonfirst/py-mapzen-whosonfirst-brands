import multiprocessing
import signal

import os
import logging
import json
import re

# used in parse_filename
pat_brand = re.compile(r"^(\d+)$")

def crawl_with_callback(source, callback, **kwargs):

    iter = crawl(source, **kwargs)

    if kwargs.get('multiprocessing', False):

        processes = multiprocessing.cpu_count() * 2
        pool = multiprocessing.Pool(processes=processes)

        def sigint_handler(signum, frame):
            logging.warning("Received interupt handler (in crawl_with_callback scope) so exiting")
            pool.terminate()
            sys.exit()

        signal.signal(signal.SIGINT, sigint_handler)

        batch = []
        batch_size = kwargs.get('multiprocessing_batch_size', 1000)

        for rsp in iter:

            batch.append((callback, rsp))

            if len(batch) >= batch_size:

                pool.map(_callback_wrapper, batch)
                batch = []

        if len(batch):
            pool.map(_callback_wrapper, batch)

    else:

        for rsp in iter:
            callback(rsp)

# Dunno - python seems all sad and whingey if this gets defined in
# the (crawl_with_callback) scope above so whatever...
# (20150902/thisisaaronland)

def _callback_wrapper(args):

    callback, feature = args

    try:
        callback(feature)
    except KeyboardInterrupt:
        logging.warning("Received interupt handler (in callback wrapper scope) so exiting")
    except Exception, e:
        logging.error("Failed to process feature because %s" % e)
        raise Exception, e
    
def load(root, brand_id):

    path = id2abspath(root, brand_id)

    fh = open(path, "r")
    return json.load(fh)

def crawl(source, **kwargs):

    validate = kwargs.get('validate', False)
    inflate = kwargs.get('inflate', False)

    for (root, dirs, files) in os.walk(source):

        for f in files:

            path = os.path.join(root, f)
            path = os.path.abspath(path)

            ret = path

            parsed = parse_filename(path)

            if not parsed:
                continue
                           
            id, suffix = parsed

            # Hey look we're dealing with an alt file of some kind!

            if suffix != None:

                if not kwargs.get('include_alt', False) and not kwargs.get('require_alt', False):
                    continue

            # OKAY... let's maybe do something?

            if validate or inflate:

                try:
                    fh = open(path, 'r')
                    data = json.load(fh)

                except Exception, e:
                    logging.error("failed to load %s, because %s" % (path, e))
                    continue

                if not inflate:
                    ret = path
                else:
                    ret = data

            yield ret

def parse_filename(path):

    fname = os.path.basename(path)
    fname, ext = os.path.splitext(fname)

    if ext != ".json":
        return None

    m = re.match(pat_brand, fname)

    if not m:
        return None

    id, = m.groups()

    return (id, None)

def id2abspath(root, id, **kwargs):

    rel = id2relpath(id, **kwargs)

    path = os.path.join(root, rel)
    return path

def id2relpath(id, **kwargs):

    fname = id2fname(id, **kwargs)
    parent = id2path(id)

    path = os.path.join(parent, fname)
    return path

def id2fname(id, **kwargs):

    return "%s.json" % id

def id2path(id):

    tmp = str(id)
    parts = []
    
    while len(tmp) > 3:
        parts.append(tmp[0:3])
        tmp = tmp[3:]

    if len(tmp):
        parts.append(tmp)

    return "/".join(parts)

def generate_id():

    url = 'http://api.brooklynintegers.com/rest/'
    params = {'method':'brooklyn.integers.create'}

    try :
        rsp = requests.post(url, params=params)    
        data = rsp.content
    except Exception, e:
        logging.error(e)
        return 0
    
    try:
        data = json.loads(data)
    except Exception, e:
        logging.error(e)
        return 0
    
    return data.get('integer', 0)
