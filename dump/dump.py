# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import pymysql
import sys,time
import traceback
sys.path.append("..")
from lib.Loging import Logging

class Dump:
    def __init__(self,**kwargs):
        self.mysql_cur = kwargs['cur']
        self.des_mysql_conn = kwargs['des_conn']
        self.des_mysql_cur = kwargs['des_cur']

        self.result = None

    def prepare_structe(self,database,tablename):
        try:
            self.des_mysql_cur.execute('CREATE DATABASE IF NOT EXISTS {}'.format(database))
        except pymysql.Warning:
            Logging(msg=traceback.format_list(), level='warning')
        except pymysql.Error:
            Logging(msg=traceback.format_list(),level='error')
            return False

        self.mysql_cur.execute('SHOW CREATE TABLE {}.{}'.format(database,tablename))
        result = self.mysql_cur.fetchall()
        create_sql = result[0]['Create Table']
        try:
            self.des_mysql_cur.execute('USE {}'.format(database))
            self.des_mysql_cur.execute('DROP TABLE IF EXISTS {}'.format(tablename))
            self.des_mysql_cur.execute(create_sql)
        except pymysql.Warning:
            Logging(msg=traceback.format_list(), level='warning')
        except pymysql.Error:
            Logging(msg=traceback.format_list(), level='error')
            return False

        return True


    def dump_to_new_db(self,database,tablename,idx,limit_num=None,end=None):
        limit_num = limit_num if limit_num else 0
        while True:
            if end and (end - limit_num) < 1000:
                sql = 'SELECT * FROM {}.{} ORDER BY {} LIMIT {},{}'.format(database, tablename, idx, limit_num,(end-limit_num))
                self.__get_from_source_db_list(sql=sql)
            else:
                sql = 'SELECT * FROM {}.{} ORDER BY {} LIMIT {},%s'.format(database,tablename,idx,limit_num)
                self.__get_from_source_db_limit1000(sql=sql)
            all_value = []
            if self.result:
                _len = len(self.result[0])
                _num = len(self.result)
                for row in self.result:
                    all_value += row.values()
            else:
                Logging(msg='return value is empty',level='warning')
                break

            sql = 'INSERT INTO {}.{} VALUES{}'.format(database,tablename,self.__combination_value_format(_len=_len,_num=_num))

            try:
                self.des_mysql_cur.execute(sql,all_value)
                self.des_mysql_conn.commint()
            except pymysql.Warning:
                Logging(msg=traceback.format_list(),level='warning')
            except pymysql.Error:
                Logging(msg=traceback.format_list(),level='error')
                self.__retry_(sql,all_value)

            if self.result < 1000:
                break
            limit_num += 1000

    def __retry_(self,sql,all_value):
        '''单个事务失败重试三次，如果都失败将退出整个迁移程序'''
        retry_num = 0
        while retry_num < 3:
            try:
                self.des_mysql_cur.execute(sql,all_value)
                break
            except pymysql.Error:
                Logging(msg=traceback.format_list(), level='error')
                time.sleep(1)
        else:
            sys.exit()


    def __get_from_source_db_list(self,sql):
        try:
            self.mysql_cur.execute(sql)
            self.result = self.mysql_cur.fetchall()
        except pymysql.Error:
            Logging(msg=traceback.format_list(),level='error')
            sys.exit()

    def __get_from_source_db_limit1000(self,sql):
        try:
            self.mysql_cur.execute(sql,1000)
            self.result = self.mysql_cur.fetchall()
        except pymysql.Error:
            Logging(msg=traceback.format_list(),level='error')
            sys.exit()

    def __combination_value_format(self,_len,_num):
        '''拼接格式化字符'''
        one_format = '({})'.format(','.join(['%s' for i in range(_len)]))
        all_ = ','.join(one_format for i in range(_num))
        return all_
