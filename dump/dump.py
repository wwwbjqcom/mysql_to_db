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


    def dump_to_new_db(self,database,tablename,idx,pri_idx,chunk_list=None,bytes_col_list=None):
        for list in chunk_list:
            start_num = list[0]
            end_num = list[1]
            limit_num = 0
            while True:
                '''
                第一次使用分块大小limit N,M, N代表起始个数位置（chunks大小），M代表条数
                第一次执行之后获取最大主键或唯一索引值范围查找
                每个线程查询一次累加条数当剩余条数小于1000时调用__get_from_source_db_list
                每个chunk剩余条数大于1000固定调用__get_from_source_db_limit1000
                '''
                sql = 'SELECT * FROM {}.{} WHERE {}>=%s and {}<=%s ORDER BY {} LIMIT {},%s'.format(database, tablename,
                                                                                                   idx, idx, idx, limit_num)
                self.__get_from_source_db_limit5000(sql=sql, args_value=[start_num, end_num])
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
                每次循环结束计算该线程还剩未处理的条数（limit_num）
                当返回条数少于1000条时将退出整个循环
                '''
                return_len = len(self.result)
                limit_num += return_len
                if return_len < 5000:
                    break
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
                Logging(msg='retry ...............',level='warning')
                self.des_mysql_cur.execute(sql,all_value)
                self.des_mysql_conn.commit()
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

    def __get_from_source_db_limit5000(self,sql,args_value):
        try:
            if args_value:
                self.mysql_cur.execute(sql, args_value + [5000])
            else:
                self.mysql_cur.execute(sql,5000)
            self.result = self.mysql_cur.fetchall()
        except pymysql.Error:
            Logging(msg=traceback.format_list(),level='error')
            sys.exit()

    def __split_data(self,start,end,idx_name,db,table):
        '''
        该函数主要作用是在数据量大时在各个线程中继续对数据进行分区
        在大于10000条时才会进行继续拆分
        用10000作为拆分的基数条件获取模糊的分区数
        再利用去重后的索引值获取每个分区最大最小值
        :param start:
        :param end:
        :param idx_name:
        :param db:
        :param table:
        :return:
        '''
        sql = 'select {} from {}.{} where {}>=%s and {}<=%s'.format(idx_name,db,table,idx_name,idx_name)
        self.mysql_cur.execute(sql,args=[start,end])
        result = self.mysql_cur.fetchall()
        values = [v[idx_name] for v in result]
        total_rows = len(values)
        split_list = []
        if total_rows > 20000:
            split_chunks = int(total_rows/10000)
            _sort_values = sorted(set(values), key=values.index)
            _nums = int(_sort_values/split_chunks)
            _n = 0
            for v in range(split_chunks):
                if v == (split_chunks -1):
                    _t = _sort_values[_n:-1]
                    split_list.append([_t[0],_t[-1]])
                else:
                    _t = _sort_values[_n:_n+_nums]
                    split_list.append([_t[0],_t[-1]])
                _n += _nums

        else:
            split_list.append[start,end]
        return split_list

    def __combination_value_format(self,_len,_num):
        '''拼接格式化字符'''
        one_format = '({})'.format(','.join(['%s' for i in range(_len)]))
        all_ = ','.join(one_format for i in range(_num))
        return all_
