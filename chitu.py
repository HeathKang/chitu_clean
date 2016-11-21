# -*- coding: utf-8 -*-

"""
ChiTu main

ChiTu is the horse

"""

from __future__ import absolute_import

import os
import sys

# find the plugins
sys.path.append(os.getcwd())

from logbook import Logger

log = Logger('main')

from maboio.lib.setup_logger import setup_logger
from maboio.lib.opts import get_option_parser
from maboio.lib.utils import get_conf

from lib.messenger import Messenger

    
def main():
    """ main """
    
    appname = "chitu"
    
    parser = get_option_parser(appname)    
    options, args = parser.parse_args()    

    conf_file = os.path.abspath(options.config)    
    
    conf = get_conf(conf_file)
    
    setup_logger(conf['logging'])
    
    log.debug("start...")
    
    ### messenger who send msg to influxdb
    messenger = Messenger(conf)
    
    messenger.run()


if __name__ == '__main__':
    main()
