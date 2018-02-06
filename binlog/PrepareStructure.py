# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import sys
sys.path.append("..")
from lib.InitDB import InitMyDB

class GetStruct:
    def __init__(self,host=None,port=None,user=None,passwd=None,socket=None):
        self.connection = InitMyDB(mysql_host=host,mysql_port=port,mysql_user=user,mysql_password=passwd,unix_scoket=socket)
        self.cur = self.connection.cursor()

    def GetColumn(self,*args):
        '''args顺序 database、tablename'''
        column_list = []
        pk_idex = None

        sql = 'select COLUMN_NAME,COLUMN_KEY from INFORMATION_SCHEMA.COLUMNS where table_schema=%s and table_name=%s order by ORDINAL_POSITION;'
        self.cur.execute(sql,args=args)
        result = self.cur.fetchall()
        for idex,row in enumerate(result):
            column_list.append(row['COLUMN_NAME'])
            if row['COLUMN_KEY'] == 'PRI':
                pk_idex = idex
        self.cur.close()
        self.connection.close()
        return column_list,pk_idex
