# -*- coding: utf-8 -*-
import json

import requests


class Kakou(object):
    def __init__(self, **kwargs):
        self.base_path = 'http://{0}:{1}{2}'.format(
            kwargs['host'], kwargs['port'], kwargs['path'])
        self.headers = {
            'content-type': 'application/json',
            'apikey': kwargs['apikey']
        }

        self.status = False

    def get_stat(self, st, et, kkdd, fxbh):
        """根据时间,地点,方向获取车流量"""
        url = '%s/stat?q={"st":"%s","et":"%s","kkdd":"%s","fxbh":"%s"}' % (
            self.base_path, st, et, kkdd, fxbh)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)['count']
            else:
                self.status = False
                raise Exception(u'url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_kakou(self, start_id, end_id, page=1, per_page=100):
        """根据ID范围获取卡口信息"""
        url = '%s/cltx?q={"page":%s,"per_page":%s,"startid":%s,"endid":%s}' % (
            self.base_path, page, per_page, start_id, end_id)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception(u'url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_kakou_by_id(self, _id):
        """根据ID范围获取卡口信息"""
        url = '{0}/cltx/{1}'.format(self.base_path, _id)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception(u'url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_maxid(self):
        """获取cltx表最大id值"""
        url = '{0}/cltx/maxid'.format(self.base_path)
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return json.loads(r.text)['maxid']
            else:
                self.status = False
                raise Exception(u'url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

