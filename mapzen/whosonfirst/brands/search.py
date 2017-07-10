import os
import os.path
import json
import logging
import copy

# import math
# import tempfile
# import types
# import urllib
# import requests

import mapzen.whosonfirst.brands.utils
import mapzen.whosonfirst.elasticsearch

class index(mapzen.whosonfirst.elasticsearch.index):

    def __init__(self, **kwargs):

        mapzen.whosonfirst.elasticsearch.index.__init__(self, **kwargs)

        self.doctype = 'brand'

    def prepare_brand(self, brand):

        id = brand['wof:brand_id']

        doctype = 'brand'
        body = self.prepare_geojson(feature)

        return {
            'id': id,
            'index': self.index,
            'doc_type': doctype,
            'body': body
        }

    # https://stackoverflow.com/questions/20288770/how-to-use-bulk-api-to-store-the-keywords-in-es-by-using-python

    def prepare_brand_bulk(self, brand):
       
        id = brand['wof:brand_id']

        doctype = 'brand'

        body = self.prepare_json(brand)

        return {
            '_id': id,
            '_index': self.index,
            '_type': doctype,
            '_source': body
        }

    def prepare_json(self, brand):

        data = copy.deepcopy(brand)
        # return self.enstringify(data)
        return data

    def enstringify(self, data, **kwargs):
        pass

    def load_file(self, f):

        try:
            fh = open(f, 'r')
            return json.load(fh)
        except Exception, e:
            logging.error("failed to open %s, because %s" % (f, e))
            raise Exception, e

    def prepare_file(self, f):

        data = self.load_file(f)
        data = self.prepare_brand(data)
        return data

    def prepare_file_bulk(self, f):

        logging.debug("prepare file %s" % f)

        data = self.load_file(f)

        data = self.prepare_brand_bulk(data)
        logging.debug("yield %s" % data)

        return data

    def prepare_files_bulk(self, files):

        for path in files:

            logging.debug("prepare file %s" % path)

            data = self.prepare_file_bulk(path)
            logging.debug("yield %s" % data)

            yield data

    def index_file(self, path):

        path = os.path.abspath(path)
        data = self.prepare_file(path)

        return self.index_document(data)

    def index_files(self, files):

        iter = self.prepare_files_bulk(files)

        return self.index_documents_bulk(iter)

    def index_filelist(self, path, **kwargs):

        def mk_files(fh):
            for ln in fh.readlines():
                path = ln.strip()

                logging.debug("index %s" % path)
                yield path

        fh = open(path, 'r')
        files = mk_files(fh)

        iter = self.prepare_files_bulk(files)
        return self.index_documents_bulk(iter)

    def delete_brand(self, brand):

        id = props['wof:brand_id']

        kwargs = {
            'id': id,
            'index': self.index,
            'doc_type': self.doctype,
            'refresh': True
        }

        self.delete_document(kwargs)

class search(mapzen.whosonfirst.elasticsearch.search):

    def __init__(self, **kwargs):

        mapzen.whosonfirst.elasticsearch.query.__init__(self, **kwargs)

    def enbrandify(self, row):

        brand = row['_source']
        return brand

class query(search):

    def __init__(self, **kwargs):

        logging.warning("mapzen.whosonfirst.search.query is deprecated - please use mapzen.whosonfirst.search.search")
        search.__init__(self, **kwargs)

if __name__ == '__main__':

    print "Please rewrite me"
