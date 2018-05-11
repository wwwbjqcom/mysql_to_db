# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import pymysql
import traceback
import sys
sys.path.append("..")
from lib.InitDB import InitMyDB
from lib.Loging import Logging

class Prepare(object):
    def __init__(self,threads,src_kwargs,des_kwargs):
        self.threads = threads
        self.db_conn_info = src_kwargs      #连接数据需要的基本信息
        self.des_conn_info = des_kwargs
        self.thread_list = []           #连接列表
        self.des_thread_list = []       #目标库链接列表
    def init_conn(self,primary_t=None):
        if primary_t:
            conn = InitMyDB(**self.db_conn_info).Init()
            if conn:
                try:
                    cur = conn.cursor()
                    state = self.__init_transaction(cur=cur,primary_t=True)
                    if state is None:
                        sys.exit()
                    return conn,cur
                except pymysql.Error:
                    Logging(msg=traceback.format_exc(), level='error')
                    sys.exit()
        else:
            for i in range(self.threads):
                conn = InitMyDB(**self.db_conn_info).Init()
                if conn:
                    try:
                        cur = conn.cursor()
                        state = self.__init_transaction(cur=cur)
                        if state:
                            self.thread_list.append({'conn':conn,'cur':cur})
                    except:
                        Logging(msg=traceback.format_exc(),level='error')

    def init_des_conn(self):
        for i in range(self.threads):
            conn = InitMyDB(**self.des_conn_info).Init()
            if conn:
                try:
                    cur = conn.cursor()
                    self.des_thread_list.append({'conn': conn, 'cur': cur})
                except:
                    Logging(msg=traceback.format_exc(), level='error')

    def master_info(self,cur):
        '''获取master信息'''
        cur.execute('SHOW MASTER STATUS')
        result = cur.fetchall()
        return result[0]['File'],result[0]['Position']

    def __init_transaction(self,cur,primary_t=None):
        '''初始化各连接事务级别'''
        try:
            cur.execute('SET SESSION wait_timeout = 2147483;')
            if primary_t:
                cur.execute('FLUSH TABLES WITH READ LOCK;')
            cur.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;')
            cur.execute('START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */;')
            return True
        except pymysql.Error:
            Logging(msg=traceback.format_exc(),level='error')
            return None

    def close(self,cur,conn):
        cur.close()
        conn.close()

    def get_chunks(self,cur,databases,tables):
        cur.execute('select count(*) as count from {}.{}'.format(databases,tables))
        result = cur.fetchall()
        total_rows = result[0]['count']
        chunk = total_rows / len(self.thread_list)
        #more_num =total_rows - (chunk * len(self.thread_list))
        return chunk


    def check_pri(self,cur,db,table):
        '''选取主键或第一个唯一索引作为导数据的条件'''
        sql = 'SHOW INDEX FROM {}.{}'.format(db,table)
        cur.execute(sql)
        result = cur.fetchall()
        if result:
            for idx in result:
                if idx['Key_name'] == 'PRIMARY' and idx['Seq_in_index'] == 1:
                    return idx['Column_name']
            else:
                if idx['Non_unique'] == 0 and idx['Seq_in_index'] == 1:
                    return idx['Column_name']
        Logging(msg='there is no suitable index to choose from {}.{},'.format(db,table),level='error')
        sys.exit()

    def get_tables(self,cur,db):
        sql = 'select table_name from tables where table_schema = %s;'
        cur.execute(sql,db)
        result = cur.fetchall()
        return result[0]['table_name']


