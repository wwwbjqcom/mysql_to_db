# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import sys,threading,socket
from GetBinlog import GetBinlog
sys.path.append("..")
from config.get_config import GetConf
from lib.Loging import Logging

encoding = 'utf-8'
BUFSIZE = 1024

class Reader(threading.Thread):
    def __init__(self, client):
        threading.Thread.__init__(self)
        self.client = client
    def run(self):
        while True:
            data = self.client.recv(BUFSIZE)
            '''string结构: [getbinglog:10010,binlog_file:'',start_position:11]'''
            string = eval(bytes.decode(data, encoding))
            Logging(msg='{}'.format(string), level='info')
            if string['getbinlog'] == 10010:
                Logging(msg='Get binlog to mha server......',level='info')
                GetBinlog(binlog_file=string['binlog_file'],start_position=int(string['start_position']),socket_client=self.client)



class Listener(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", port))
        self.sock.listen(0)

    def run(self):
        Logging(msg='socket listener started',level='info')
        while True:
            client, cltadd = self.sock.accept()
            Reader(client).start()
            Logging(msg='accept a connect',level='info')

class Socket:
    def __init__(self):
        pass
    def start(self):
        lst = Listener(GetConf().GetSockPort())
        lst.start()