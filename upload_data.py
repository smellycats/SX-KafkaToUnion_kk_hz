# -*- coding: utf-8 -*-
import time
import json
import math
import socket
from concurrent.futures import ProcessPoolExecutor as Pool

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

        self.local_ip = socket.gethostbyname(socket.gethostname())  # 本地IP

        self.part_list = list(range(60))

        self.workers = dict(self.my_ini['sys'])['workers']

        self.weight = dict(self.my_ini['sys'])['weight']
        self.last_items = 0
        self.lost_data = []

    def upload_data(self, weight):
        items = []
        offsets = {}
        pool = Pool(max_workers=self.workers)
        for i in range(weight*100):
            msg = self.kc.c.poll(0.005)
        
            if msg is None:
                continue
            if msg.error():
                continue
            else:
                m = json.loads(msg.value().decode('utf-8'))['message']
                m['img_path'] = m['imgurl']
                m['fxbh'] = m['fxbh_id']
                items.append(m)
            par = msg.partition()
            off = msg.offset()
            offsets[par] = off
        if offsets == {}:
            return 0
        else:
            #print('items={0}'.format(len(items)))
            #logger.info('items={0}'.format(len(items)))
            if len(items) > 0:
                results = []
                z = math.ceil(float(len(items))/100)
                for i in range(z):
                    data = items[i*100:(i+1)*100]
                    obj = pool.submit(self.uk.post_data, data)
                    results.append(obj)
                pool.shutdown()
                result = [obj.result().status_code for obj in results]
                print(result)
                logger.info(result)
                info = '{0}, items={1}'.format(items[-1]['jgsj'], len(items))
                print(info)
                logger.info(info)
                #self.uk.post_kakou(items)                 # 上传数据
            self.kc.c.commit(async=False)
            #print(offsets)
            logger.info(offsets)
            return len(items)

    def main_loop(self):
        while 1:
            try:
                if self.kc is None:
                    self.kc = KafkaConsumer(**dict(self.my_ini['kafka']))
                    self.kc.assign(self.part_list)
                if self.uk is not None and self.uk.status:
                    self.upload_data(self.weight)
                else:
                    self.uk = UnionUpload(**dict(self.my_ini['union']))
                    self.uk.status = True
            except Exception as e:
                logger.exception(e)
                time.sleep(15)

        
