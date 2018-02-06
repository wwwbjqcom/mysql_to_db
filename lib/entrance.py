# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import sys
sys.path.append("..")
from binlog import Metadata
from OperationDB import OperationDB
from InitDB import InitMyDB


class Entrance(Metadata.TableMetadata):
    def __init__(self,kargs):
        self.binlog_file,self.start_position = kargs['file'],kargs['start-position']
        self.host = kargs['host'] if 'host' in kargs else '127.0.0.1'
        self.port = kargs['port']  if 'port' in kargs else 3306
        self.user = kargs['user']
        self.passwd = kargs['passwd']
        self.socket = kargs['socket'] if 'socket' in kargs else None

        self.databases = kargs['databases'].split(',')
        self.tables = kargs['tables'].split(',') if 'tables' in kargs else None

        self.d_host = kargs['dhost']
        self.d_port = kargs['dport']
        self.d_user = kargs['duser']
        self.d_passwd = kargs['dpasswd']

    def __enter__(self):
        OperationDB(databases=self.databases,tables=self.tables,binlog_file=self.binlog_file,start_position=self.start_position,
                    host=self.host,port=self.port,user=self.user,passwd=self.passwd,dhost=self.d_host,dport=self.d_port,
                    duser=self.d_user,dpasswd=self.d_passwd,socket=self.socket).Operation()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

