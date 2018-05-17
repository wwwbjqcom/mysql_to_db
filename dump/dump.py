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
        '''
        在目标库准备对于的数据库、表结构
        目标库的数据表如果存在将直接删除
        如果目标表有数据需要注意是否可以直接删除
        :param database:
        :param tablename:
        :return:
        '''
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


    def dump_to_new_db(self,database,tablename,idx,pri_idx,start_num=None,limit_num=None):
        start_num = start_num if start_num else 0
        __init_stat = []
        while True:
            '''
            第一次使用分块大小limit N,M, N代表起始个数位置（chunks大小），M代表条数
            第一次执行之后获取最大主键或唯一索引值范围查找
            每个线程查询一次累加条数当剩余条数小于1000时调用__get_from_source_db_list
            每个chunk剩余条数大于1000固定调用__get_from_source_db_limit1000
            '''
            if __init_stat:
                if limit_num and limit_num>=1000:
                    sql = 'SELECT * FROM {}.{} WHERE {} ORDER BY {} LIMIT 0,%s'.format(database, tablename,
                                                                                       self.__join_pri_where(pri_idx),
                                                                                       idx)
                    self.__get_from_source_db_limit1000(sql=sql, pri_value=__init_stat)
                elif limit_num and limit_num < 1000:
                    sql = 'SELECT * FROM {}.{} WHERE {} ORDER BY {} LIMIT 0,{}'.format(database, tablename,
                                                                                       self.__join_pri_where(pri_idx),
                                                                                       idx,
                                                                                       limit_num)
                    self.__get_from_source_db_list(sql=sql, pri_value=__init_stat)
                else:
                    sql = 'SELECT * FROM {}.{} WHERE {} ORDER BY {} LIMIT 0,%s'.format(database, tablename,
                                                                                       self.__join_pri_where(pri_idx),
                                                                                       idx)
                    self.__get_from_source_db_limit1000(sql=sql, pri_value=__init_stat)
            else:
                if limit_num and limit_num >= 1000:
                    sql = 'SELECT * FROM {}.{} ORDER BY {} LIMIT {},%s'.format(database, tablename, idx, start_num)
                    self.__get_from_source_db_limit1000(sql=sql)
                elif limit_num and limit_num < 1000:
                    sql = 'SELECT * FROM {}.{} ORDER BY {} LIMIT {},{}'.format(database, tablename, idx, start_num,
                                                                               limit_num)
                    self.__get_from_source_db_list(sql=sql)
                else:
                    sql = 'SELECT * FROM {}.{} ORDER BY {} LIMIT {},%s'.format(database, tablename, idx, start_num)
                    self.__get_from_source_db_limit1000(sql=sql)
            '''======================================================================================================'''

            '''
            拼接1000行数据为pymysql格式化列表
            如果返回数据为空直接退出
            '''
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
                self.des_mysql_conn.commit()
            except pymysql.Warning:
                Logging(msg=traceback.format_list(),level='warning')
            except pymysql.Error:
                Logging(msg=traceback.format_list(),level='error')
                self.__retry_(sql,all_value)

            '''
            获取每次获取数据的最大主键或唯一索引值
            '''
            __init_stat = []
            _end_value = self.result[-1]
            for col in pri_idx:
                _a = [v for v in col.keys()]
                __init_stat.append(_end_value[_a[0]])
            '''========================================='''

            '''
            每次循环结束计算该线程还剩未处理的条数（limit_num）
            当返回条数少于1000条时将退出整个循环
            '''
            if len(self.result) < 1000:
                break
            if limit_num:
                limit_num = limit_num - 1000
                if limit_num == 0:
                    return
            '''=========================================='''

    def __join_pri_where(self,pri_key_info):
        '''
        if len(pri_key_info) > 1:
            _keys = [[_k for _k in col.keys()] for col in pri_key_info]
            return ' AND '.join(['`{}`>=%s'.format(k[0]) for k in _keys])
        '''
        _keys = [[_k for _k in col.keys()] for col in pri_key_info]
        return '`{}`>%s'.format(_keys[0][0])

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


    def __get_from_source_db_list(self,sql,pri_value=None):
        try:
            self.mysql_cur.execute(sql,pri_value)
            self.result = self.mysql_cur.fetchall()
        except pymysql.Error:
            Logging(msg=traceback.format_list(),level='error')
            sys.exit()

    def __get_from_source_db_limit1000(self,sql,pri_value=None):
        try:
            if pri_value:
                self.mysql_cur.execute(sql, pri_value + [1000])
            else:
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
