# -*- coding: utf-8 -*-

"""
Messenger

"""
# find the plugins


import time
import traceback

import msgpack
from sys import version_info
from redis.exceptions import ConnectionError, NoScriptError
from maboio.lib.influxdb_lib import InfluxC
from maboio.lib.redis_lib import RedisClient
from logbook import Logger

log = Logger('msger')


class Messenger(object):
    """   messenger """

    def __init__(self, conf):
        """ init """

        self.interval = 1
        self.red = RedisClient(conf['redis'])
        self.influxc = InfluxC(conf['influxdb'])
        self.dead_threshold = conf['app']['dead_threshold']
        self.node_name = conf['app']['node_name']

    def run(self):
        """ send msg to influxdb  """
        dead_count = 0
        last_alive_time = time.time()


        while True:
            if dead_count > 5:
                dead_json = {'time': int(time.time()),
                             'measurement': 'dead_node',
                             'fields': {'node_name': self.node_name},
                             'tags': {'alive?': 'no'}
                             }
                self.influxc.send([dead_json])
                log.debug('the node is dead now')
            try:

                data_len = self.red.get_len()
                if data_len > 0:
                    alive_json = {'time': int(time.time()),
                                  'measurement': 'dead_node',
                                  'fields': {'node_name': self.node_name},
                                  'tags': {'alive?': 'yes'}
                                  }
                    self.influxc.send([alive_json])
                    dead_count = 0
                    last_alive_time = time.time()
                    for i in range(0, data_len):

                        rdata = self.red.dequeue()
                        data = transefer(msgpack.unpackb(rdata[1]))

                        # log.debug(data)

                        # string to integer
                        data['data']['time'] = int(data['data']['time'])

                        json_data = [data['data']]

                        log.debug(json_data)

                        try:
                            self.influxc.send(json_data)
                        except Exception as ex:

                            log.error(ex)
                            log.error(traceback.format_exc())

                            log.debug("re queue...")
                            self.red.re_queue(rdata)
                            time.sleep(6)
                            continue
                else:
                    log.debug('queue now don\'t have data')
                    now = time.time()
                    threshold = self.dead_threshold * 1
                    if now - last_alive_time > threshold:
                        dead_count += 1

            except Exception as ex:
                log.error(ex)
                log.error(traceback.format_exc())

            # timer("output")
            time.sleep(self.interval)


def transefer(bytes_dict):
    """
    lua to python3, lua's table will be transefer to python dict, but the key
    and the value of dict is byte string, and bytes string can't be directly
    used in send function from influxdb package.
    :param bytes_dict: a dict whcih key and value is byte string.
    :return: a user-friendly normal dict.
    """
    a = {}
    if not isinstance(bytes_dict, dict):
        return bytes_dict
    for key, value in bytes_dict.items():
        value = transefer(value)
        if isinstance(key, bytes):
            key = key.decode()
        if isinstance(value, bytes):
            value = value.decode()
        a[key] = value
    return a
