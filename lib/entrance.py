# -*- encoding: utf-8 -*-
'''
@author: Great God
'''
import sys
sys.path.append("..")
from binlog import Metadata
from .OperationDB import OperationDB


class Entrance(Metadata.TableMetadata):
    def __init__(self,kargs):
        self.binlog_file = kargs['file'] if 'file' in kargs else None
        self.start_position = kargs['start-position'] if 'start-position' in kargs else None
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
        self.ignore_type = kargs['ignore_type'] if 'ignore_type' in kargs else None
        self.server_id = kargs['serverid'] if 'serverid' in kargs else 133
        self.binlog = kargs['binlog'] if 'binlog' in kargs else None

        self.ithread = kargs['ithread'] if 'ithread' in kargs else None
        self.full_dump = kargs['full'] if 'full' in kargs else None
        self.threads = kargs['threads'] if self.full_dump and 'threads' in kargs else None
    def __enter__(self):
        OperationDB(databases=self.databases,tables=self.tables,binlog_file=self.binlog_file,start_position=self.start_position,
                    host=self.host,port=self.port,user=self.user,passwd=self.passwd,dhost=self.d_host,dport=self.d_port,
                    duser=self.d_user,dpasswd=self.d_passwd,socket=self.socket,ignore_type=self.ignore_type,
                    server_id=self.server_id,binlog=self.binlog,full_dump=self.full_dump,threads=self.threads,
                    ithread=self.ithread).Operation()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

