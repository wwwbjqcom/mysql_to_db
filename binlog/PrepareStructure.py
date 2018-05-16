# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import sys
sys.path.append("..")
from lib.InitDB import InitMyDB

class GetStruct:
    def __init__(self,host=None,port=None,user=None,passwd=None,socket=None):
        self.connection = InitMyDB(mysql_host=host,mysql_port=port,mysql_user=user,mysql_password=passwd,unix_socket=socket).Init()
        self.cur = self.connection.cursor()
        self.cur.execute('set sql_log_bin=0;')

    def GetColumn(self,*args):
        '''args顺序 database、tablename'''
        column_list = []
        column_type_list = []
        pk_idex = None

        sql = 'select COLUMN_NAME,COLUMN_KEY,COLUMN_TYPE from INFORMATION_SCHEMA.COLUMNS where table_schema=%s and table_name=%s order by ORDINAL_POSITION;'
        self.cur.execute(sql,args=args)
        result = self.cur.fetchall()
        pk_idex = []
        for idex,row in enumerate(result):
            column_list.append(row['COLUMN_NAME'])
            column_type_list.append(row['COLUMN_TYPE'])
            if row['COLUMN_KEY'] == 'PRI':
                pk_idex.append(idex)
        return column_list,pk_idex,column_type_list

    def CreateTmp(self):
        self.cur.execute('CREATE DATABASE IF NOT EXISTS dump2db;')                                                                      #创建临时库
        self.cur.execute('DROP TABLE IF EXISTS dump2db.dump_status;')                                                                           #删除表
        self.cur.execute('CREATE TABLE dump2db.dump_status(id INT,exe_gtid VARCHAR(50),logname VARCHAR(100),at_pos BIGINT,next_pos BIGINT,PRIMARY KEY(id));')    #创建临时表

    def SaveStatus(self,logname,at_pos,next_pos,server_id,gtid=None):
        if gtid:
            sql = 'INSERT INTO dump2db.dump_status(id,exe_gtid,logname,at_pos,next_pos) VALUES(%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE exe_gtid=%s,logname=%s,at_pos=%s,next_pos=%s;'
        else:
            sql = 'INSERT INTO dump2db.dump_status(id,logname,at_pos,next_pos) VALUES(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE logname=%s,at_pos=%s,next_pos=%s;'
        self.cur.execute(sql,(server_id,logname,at_pos,next_pos,logname,at_pos,next_pos))
        self.connection.commit()

    def close(self):
        self.cur.close()
        self.connection.close()


