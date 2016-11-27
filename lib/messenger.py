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
        if isinstance(value, dict):
            a[key.decode()] = value
        else:
            a[key.decode()] = value.decode()
    return a


class Messenger(object):
    """   messenger """

    def __init__(self, conf):
        """ init """

        self.interval = 1
        self.red = RedisClient(conf['redis'])
        self.influxc = InfluxC(conf['influxdb'])

    def run(self):
        """ send msg to influxdb  """

        while True:

            try:

                data_len = self.red.get_len()

                if data_len > 0:

                    for i in range(0, data_len):

                        rdata = self.red.dequeue()
                        if version_info[0] == 3:
                            data = transefer(msgpack.unpackb(rdata[1]))
                        else:
                            data = msgpack.unpackb(rdata[1])

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

            except Exception as ex:
                log.error(ex)
                log.error(traceback.format_exc())

            # timer("output")
            time.sleep(self.interval)
