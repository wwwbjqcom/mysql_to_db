# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import sys
sys.path.append("..")
from lib.Loging import Logging
from binlog.ParseEvent import ParseEvent
from binlog.Metadata import binlog_event_header_len


class GetBinlog:
    def __init__(self,binlog_file=None,start_position=None,socket_client=None):
        self.binlog_file = binlog_file
        self.start_position = start_position
        self.socket = socket_client
        self.binlog_dir = GetConf().GetBinlogDir()
        self.__start()

    def __start(self):
        binlog_file_dir = '{}/{}'.format(GetConf().GetBinlogDir(),self.binlog_file)
        p_event = ParseEvent(filename=binlog_file_dir,startpostion=self.start_position)
        p_event.file_data.seek(p_event.startposition)
        while True:
            code,event_length = p_event.read_header()
            Logging(msg='code :{}, event_length: {}'.format(code,event_length),level='info')
            if code is None:
                self.socket.send('{"binlogvalue":10010}')
                Logging(msg='OK!', level='info')
                break
            else:
                p_event.file_data.seek(-binlog_event_header_len,1)
                self.socket.send(p_event.file_data.read(event_length))
                while True:
                    stat = self.socket.recv(100)
                    if stat and int(eval(stat)['recv_stat']) == 119:
                        break
                    else:
                        self.socket.send(p_event.file_data.read(event_length))

