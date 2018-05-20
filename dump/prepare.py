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
        '''
        初始化数据库链接，所有链接添加到链接列表
        :param primary_t:
        :return:
        '''
        if primary_t:
            conn = InitMyDB(**self.db_conn_info).Init()
            if conn:
                try:
                    cur = conn.cursor()
                    state = self.__init_transaction(cur=cur,primary_t=True)
                    if state is None:
                        sys.exit()
                    self.thread_list.append({'conn': conn, 'cur': cur})
                    return conn,cur
                except pymysql.Error:
                    Logging(msg=traceback.format_exc(), level='error')
                    sys.exit()
        else:
            for i in range(self.threads-1):
                conn = InitMyDB(**self.db_conn_info).Init()
                if conn:
                    try:
                        cur = conn.cursor()
                        state = self.__init_transaction(cur=cur)
                        if state:
                            self.thread_list.append({'conn':conn,'cur':cur})
                    except:
                        Logging(msg=traceback.format_exc(),level='error')

    def init_des_conn(self,binlog=None):
        '''
        在线导出时用于初始化目标库的链接
        默认不记录binlog，需指定--binlog参数才能记录binlog
        session链接timeout时间为2147483
        :param binlog:
        :return:
        '''
        for i in range(self.threads-1):
            conn = InitMyDB(**self.des_conn_info).Init()
            if conn:
                try:
                    cur = conn.cursor()
                    if binlog is None:
                        cur.execute('set sql_log_bin=0;')
                    cur.execute('SET SESSION wait_timeout = 2147483;')
                    self.des_thread_list.append({'conn': conn, 'cur': cur})
                except:
                    Logging(msg=traceback.format_exc(), level='error')

    def master_info(self,cur):
        '''
        获取master信息
        :param cur:
        :return:
        '''
        cur.execute('SHOW MASTER STATUS')
        result = cur.fetchall()
        return result[0]['File'],result[0]['Position']

    def __init_transaction(self,cur,primary_t=None):
        '''
        初始化在线导出时源库所有链接的事务信息
        :param cur:
        :param primary_t:
        :return:
        '''
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
        try:
            cur.close()
            conn.close()
        except:
            pass
    def get_chunks(self,cur,databases,tables,index_name):
        '''
        获取每个线程分块在索引中的数据范围
        优先判断数据总量，如果数据总条数小于并发线程数据将使用单线程
        :param cur:
        :param databases:
        :param tables:
        :return:
        '''
        cur.execute('select {} from {}.{} group by {} order by {}'.format(index_name,databases,tables,index_name,index_name))
        result = cur.fetchall()
        total_rows = len(result)
        result_value = [row[index_name] for row in result]
        if total_rows == 0:
            return  None,None
        if total_rows < len(self.thread_list):
            return [[result_value[0],result_value[-1]]],None

        chunk = int(total_rows/(len(self.thread_list)))
        '''记录每个分块索引字段最大最小值'''
        chunks_list = []
        start = 0
        '''_tmp记录每个块的最大值，由于可能存在重复值，所以在下一个块计算时会进行比较'''
        for i in range(len(self.thread_list)):
            if i == len(self.thread_list) - 1:
                a = result_value[start:-1]
            else:
                a = result_value[start:start+chunk]
            chunks_list.append(self.__split_data(a))
            start += chunk
        return chunks_list,True

    def __split_data(self,data_list):
        '''
        按10000一个区间拆分每个线程执行的数据
        :param data_list:
        :return:
        '''
        _l = len(data_list)
        _tmp = []
        if _l > 10000:
            _n = int(_l/10000)
            _t = 0
            for v in range(_n+1):
                if v == _n:
                    _all = data_list[_t:-1]
                    _tmp.append([_all[0],_all[1]])
                else:
                    _all = data_list[_t:_t+10000]
                    _tmp.append([_all[0],_all[-1]])
                _t += 10000
        else:
            _tmp.append(data_list)
        return _tmp



    def get_max_min(self,cur,databases,tables,index_name):
        cur.execute('select min({}) as min,max({}) as max from {}.{}'.format(index_name, index_name, databases, tables))
        re_min_max = cur.fetchall()
        min = re_min_max[0]['min']
        max = re_min_max[0]['max']
        return [min, max]

    def check_pri(self,cur,db,table):
        '''
        该函数获取查询数据时where条件的字段
        获取索引字段优先级：
            首先获取表主键，如果主键有自增字段将直接使用
            如无自增字段选择主键第一个字段
            无主键选择一个唯一索引字段
            都没有的情况下选择一个过滤性最好的字段
        没有合适的索引可供选择将直接退出
        :param cur:
        :param db:
        :param table:
        :return:
        '''
        pri_name,pri_index_info = self.__get_pri_column_idx(cur=cur,db=db,table=table)
        if pri_name and pri_index_info:
            return pri_name,pri_index_info

        sql = 'SHOW INDEX FROM {}.{}'.format(db, table)
        cur.execute(sql)
        result = cur.fetchall()
        _tmp_key_name = None
        _tmp_key_card = 0
        if result:
            for idx in result:
                if idx['Key_name'] != 'PRIMARY':
                    return idx['Column_name'],self.__get_col_info(cur,db,table,idx['Column_name'])
            for idx in result:
                if idx['Non_unique'] == 0 and idx['Key_name'] != 'PRIMARY':
                    return idx['Column_name'], self.__get_col_info(cur, db, table, idx['Column_name'])
            for idx in result:
                if idx['Cardinality'] > _tmp_key_card:
                    _tmp_key_name,_tmp_key_card = idx['Column_name'],idx['Cardinality']

            return _tmp_key_name,self.__get_col_info(cur, db, table, _tmp_key_name)
        Logging(msg='there is no suitable index to choose from {}.{},'.format(db,table),level='error')
        sys.exit()

    def __get_pri_column_idx(self,cur,db,table):
        '''返回自增字段信息'''
        sql = 'select COLUMN_NAME,COLUMN_KEY,COLUMN_TYPE,EXTRA from INFORMATION_SCHEMA.COLUMNS where table_schema=%s and table_name=%s order by ORDINAL_POSITION;'
        cur.execute(sql, args=[db,table])
        result = cur.fetchall()
        for idex, row in enumerate(result):
            if row['COLUMN_KEY'] == 'PRI':
                if row['EXTRA'] == 'auto_increment':
                    return row['COLUMN_NAME'],[{row['COLUMN_NAME']:idex}]
        return None,None

    def check_byte_col(self,cur,db,table):
        '''返回二进制类型字段的index'''
        sql = 'select COLUMN_NAME,COLUMN_TYPE from INFORMATION_SCHEMA.COLUMNS where table_schema=%s and table_name=%s order by ORDINAL_POSITION; '
        cur.execute(sql,args=[db,table])
        result = cur.fetchall()
        col_list=[]
        for idex,row in enumerate(result):
            if 'blob' in row['COLUMN_TYPE'] or 'set' in row['COLUMN_TYPE'] or 'binary' in row['COLUMN_TYPE']:
                col_list.append(idex)
        return col_list

    def __get_col_info(self,cur,db,table,col):
        '''
        根据字段名获取字段所在顺序，通过该index获取对应的行值
        :param cur:
        :param db:
        :param table:
        :param col:
        :return:
        '''
        sql = 'select ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS where table_schema=%s and table_name=%s and column_name=%s;'
        cur.execute(sql, args=[db, table, col])
        result = cur.fetchall()
        if result:
            return [{col:result[0]['ORDINAL_POSITION']}]
        else:
            return None

    def get_tables(self,cur,db):
        '''
        获取对应schema下的所有数据表名称
        :param cur:
        :param db:
        :return:
        '''
        sql = 'select table_name from information_schema.tables where table_schema = %s;'
        cur.execute(sql,db)
        result = cur.fetchall()
        _tmp = [row['table_name'] for row in result]
        return _tmp

