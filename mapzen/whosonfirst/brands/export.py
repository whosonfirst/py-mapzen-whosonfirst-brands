import os
import logging
import json
import mapzen.whosonfirst.brands.utils
import atomicwrites

class flatfile:

    def __init__(self, root, **kwargs):

        path = os.path.abspath(root)        
        self.root = path

        self.debug = kwargs.get('debug', False)

    def export_feature(self, f, **kwagrs):

        return self.write_feature(self, f, **kwargs)
    
    def write_feature(self, f, **kwargs):

        indent = kwargs.get('indent', None)

        path = self.feature_path(f, **kwargs)
        root = os.path.dirname(path)

        if not os.path.exists(root):
            os.makedirs(root)

        logging.info("writing %s" % (path))

        try:

            with atomicwrites.atomic_write(path, overwrite=True) as fh:
                json.dump(f, fh, indent=indent)

        except Exception, e:
            logging.error("failed to write %s, because %s" % (path, e))
            return None

        return path

    def feature_path(self, f, **kwargs):

        wofid = f.get('wof:brand_id', None)

        if wofid == None:
            raise Exception, "Missing WOF brand ID"

        abspath = mapzen.whosonfirst.brands.utils.id2abspath(wofid, **kwargs)
        return abspath
