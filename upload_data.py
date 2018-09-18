# -*- coding: utf-8 -*-
import time
import json
import base64
import socket

import arrow

import helper
from helper_kakou_v3 import Kakou
from helper_consul import ConsulAPI
from helper_kafka_producer import KafkaProducer
from my_yaml import MyYAML
from my_logger import *


debug_logging('/home/logs/error.log')
logger = logging.getLogger('root')


class UploadData(object):
    def __init__(self):
        # 配置文件
        self.my_ini = MyYAML('/home/my.yaml').get_ini()
        self.flag_ini = MyYAML('/home/flag.yaml')

        # request方法类
        self.kk = None
        self.kp = KafkaProducer(**dict(self.my_ini['kafka_producer']))
        self.con = ConsulAPI()
        self.con.path = self.my_ini['consul']['path']
	
        # ID上传标记
        self.kk_name = dict(self.my_ini['kakou'])['name']
        self.step = dict(self.my_ini['kakou'])['step']
        self.kkdd = dict(self.my_ini['kakou'])['kkdd']

        self.local_ip = socket.gethostbyname(socket.gethostname())   # 本地IP
        #self.id_flag = self.flag_ini.get_ini()['id']
        self.bool_lost = False               # 是否存在未上传数据

    def get_id(self):
        """获取上传id"""
        r = self.con.get_id()[0]
        return json.loads(base64.b64decode(r['Value']).decode()), r['ModifyIndex']

    def set_id(self, _id, modify_index):
        """设置ID"""
        if self.con.put_id(_id, modify_index):
            logger.info(_id)

    def post_lost_data(self):
        """未上传数据重传"""
        lost_list, modify_index = self.get_lost()
        if len(lost_list) == 0:
            return 0
        now = arrow.now('PRC').format('YYYY-MM-DD HH:mm:ss')
        for i in lost_list:
            value = {'timestamp': now, 'message': i['message']}
            self.kp.produce_info(key='{0}_{0}'.format(self.kk_name, i['message']['id']), value=json.dumps(value))
            print('lost={0}'.format(i['message']['id']))
        self.ka.flush()
        lost_list = []
        if len(self.ka.lost_msg) > 0:
            for i in self.ka.lost_msg:
                lost_list.append(json.loads(i.value()))
            self.ka.lost_msg = []
        self.con.put_lost(json.dumps(lost_list))
        return len(lost_list)

    def post_info(self, id):
        """上传数据"""
        #id = self.get_id()
        info = self.kk.get_kakou(id+1, id+self.step, 1, self.step)
        
        # 如果查询数据为0
        if info['total_count'] == 0:
            maxid = self.kk.get_maxid()
            if (id+self.step) < maxid:
                return id+self.step
            return id

        lost_msg = []  #未上传数据列表
        def acked(err, msg):
            if err is not None:
                lost_msg.append(msg.value().decode('utf-8'))
                logger.error(msg.value())
                logger.error(err)
        now = arrow.now('PRC').format('YYYY-MM-DD HH:mm:ss')
        for i in info['items']:
            if i['kkdd_id'] is None or i['kkdd_id'] == '':
                continue
            else:
                if len(i['kkdd_id']) < 9:
                    continue
                if int(i['kkdd_id']) > 441303300:
                    continue
            if i['hphm'] is None or i['hphm'] == '':
                i['hphm'] = '-'
            i['clxs'] = 0
            i['cllx'] = 'X99'
            i['csys'] = 'Z'
            i['hpzl'] = helper.hphm2hpzl(i['hphm'], i['hpys_id'], i['hpzl'])
            value = {'timestamp': now, 'message': i}
            self.kp.produce_info(key='{0}_{0}'.format(self.kk_name, i['id']), value=json.dumps(value), cb=acked)
        self.kp.flush()

        if len(lost_msg) > 0:
            self.bool_lost = True
        print(info['items'][-1]['id'], info['items'][-1]['jgsj'])
        return info['items'][-1]['id'] #info['total_count']

    def main_loop(self):
        while 1:
            if self.kk is not None and self.kk.status:
                try:
                    id, modify_index = self.get_id()
                    last_id = self.post_info(id)
                    self.set_id(last_id, modify_index)
                    if last_id <= id:
                        time.sleep(1)
                except Exception as e:
                    logger.exception(e)
                    time.sleep(15)
            else:
                try:
                    if self.kk is None or not self.kk.status:
                        self.kk = Kakou(**dict(self.my_ini['kakou']))
                        self.kk.status = True
                except Exception as e:
                    logger.exception(e)
                    time.sleep(15)
        
