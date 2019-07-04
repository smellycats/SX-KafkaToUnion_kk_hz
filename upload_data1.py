# -*- coding: utf-8 -*-
import time
import json
import socket

import arrow

from helper_consul import ConsulAPI
from helper_kafka_consumer import KafkaConsumer
from helper_union_upload_v2 import UnionUpload
from my_yaml import MyYAML
from my_logger import *


debug_logging('/home/logs/error.log')
logger = logging.getLogger('root')


class UploadData(object):
    def __init__(self):
        # 配置文件
        self.my_ini = MyYAML('/home/my.yaml').get_ini()

        # request方法类
        self.kc = None
        self.uk = None
        self.con = ConsulAPI()
        #self.con.path = dict(self.my_ini['consul'])['path']

        self.item = None
        self.part_list = list(range(60))

        self.local_ip = socket.gethostbyname(socket.gethostname())  # 本地IP

    def upload_data(self):
        items = []
        offsets = {}
        for i in range(400):
            msg = self.kc.c.poll(0.005)
            #print(msg.value())       
            if msg is None:
                continue
            if msg.error():
                continue
            else:
                m = json.loads(msg.value().decode('utf-8'))['message']
                #print(m['kkdd_id'])
                if m['kkdd_id'] is None:
                    continue
                if len(m['kkdd_id']) < 9:
                    continue
                if m['kkdd_id'][:6] in ('441324', '441303') or m['kkdd_id'] in ('441302030', '441302031', '441302032', '441302033', '441302034', '441302035', '441302036', '441302037', '441302045', '441302046', '441302047'):
                    m['img_path'] = m['imgurl']
                    m['fxbh'] = m['fxbh_id']
                    items.append(m)
            par = msg.partition()
            off = msg.offset()
            offsets[par] = off
        #print(offsets)
        if offsets == {}:
            return 0
        else:
            if len(items) > 0:
                #logger.info(items)
                print('{0}, items={1}'.format(items[-1]['jgsj'], len(items)))
                self.uk.post_kakou(items)                 # 上传数据
            self.kc.c.commit(async=False)
            info_msg = 'items={0}, offset={1}'.format(len(items), offsets)
            #print(info_msg)
            logger.info(info_msg)
            return 1

    def main_loop(self):
        count = 0
        while 1:
            try:
                #if not self.get_lock():
                #    if self.kc is not None:
                #        del self.kc
                #        self.kc = None
                #    self.item = None
                #    self.part_list = []
                #    time.sleep(2)
                #    continue
                #print('test')
                if count > 3:
                    logger.info('exit')
                    exit()
                if self.kc is None:
                    self.kc = KafkaConsumer(**dict(self.my_ini['kafka']))
                    self.kc.assign(self.part_list)
                if self.uk is not None and self.uk.status:
                    n = self.upload_data()
                    if n > 0:
                        count = 0
                    else:
                        count += 1
                        logger.info('count={0}'.format(count))
                        time.sleep(1)
                else:
                    self.uk = UnionUpload(**dict(self.my_ini['union']))
                    self.uk.status = True
            except Exception as e:
                logger.exception(e)
                count += 2
                logger.info('count={0}'.format(count))
                time.sleep(15)
        
