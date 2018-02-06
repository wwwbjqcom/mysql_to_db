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
        self.socket = kargs['socket']

        self.databases = kargs['databases'].split(',')
        self.tables = kargs['tables'].split(',') if 'tables' in kargs else None

        self.d_host = kargs['dhost']
        self.d_port = kargs['dport']
        self.d_user = kargs['duser']
        self.d_passwd = kargs['dpasswd']

    def __enter__(self):

        mysql_conn = InitMyDB(mysql_host=self.host,mysql_port=self.port,mysql_user=self.user,mysql_password=self.passwd,unix_scoket=self.socket).Init()
        destination_conn = InitMyDB(mysql_host=self.d_host,mysql_port=self.d_port,mysql_user=self.d_user,mysql_password=self.d_passwd).Init()
        if mysql_conn and destination_conn:
            OperationDB(databases=self.databases,tables=self.tables,source_conn=mysql_conn,destination_conn=destination_conn,binlog_file=self.binlog_file,start_position=self.start_position)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

