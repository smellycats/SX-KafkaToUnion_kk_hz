# -*- coding: utf-8 -*-
import json

import requests


class UnionUpload(object):

    def __init__(self, **kwargs):
        self.base_path = 'http://{0}:{1}{2}'.format(
            kwargs['host'], kwargs['port'], kwargs['path'])
        self.headers = {
            'content-type': 'application/json',
            'apikey': kwargs['apikey']
        }

        self.status = False
        
    def post_kakou(self, data):
        url = '{0}'.format(self.base_path)
        #print(url)
        try:
            r = requests.post(url, headers=self.headers, data=json.dumps(data))
            if r.status_code == 201:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_root(self):
        url = '{0}'.format(self.base_path)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise
