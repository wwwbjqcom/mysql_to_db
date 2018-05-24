# -*- encoding: utf-8 -*-
'''
@author: Great God
'''
import logging
logging.basicConfig(filename='server.log',
                    level=logging.INFO,
                    format  = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %A %H:%M:%S')

def Logging(msg,level):
    if level == 'error':
        logging.error(msg)
    elif level == 'warning':
        logging.warning(msg)
    elif level == 'info':
        logging.info(msg)
    else:
        logging.warning('not support this level {}'.format(level))