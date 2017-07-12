import os
import logging
import json
import mapzen.whosonfirst.brands.utils
import atomicwrites
import time

class flatfile:

    def __init__(self, root, **kwargs):

        path = os.path.abspath(root)        
        self.root = path

        self.debug = kwargs.get('debug', False)

    def export_brand(self, b, **kwargs):

        return self.write_brand(b, **kwargs)
    
    def write_brand(self, b, **kwargs):

        now = int(time.time())

        b["wof:lastmodified"] = now
        
        indent = kwargs.get('indent', 2)

        path = self.brand_path(b, **kwargs)
        root = os.path.dirname(path)

        if not os.path.exists(root):
            os.makedirs(root)

        logging.info("writing %s" % (path))

        fh = open(path, "w")
        json.dump(b, fh, indent=indent)
        fh.close()

        """
        try:

            with atomicwrites.atomic_write(path, overwrite=True) as fh:
                json.dump(b, fh, indent=indent)

        except Exception, e:
            logging.error("failed to write %s, because %s" % (path, e))
            return None
        """
        
        return path

    def brand_path(self, b, **kwargs):
        
        brand_id = b.get("wof:brand_id", None)

        if brand_id == None:
            raise Exception, "Missing WOF brand ID"

        abspath = mapzen.whosonfirst.brands.utils.id2abspath(self.root, brand_id, **kwargs)
        return abspath
