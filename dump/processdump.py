# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import queue
import traceback
import threading
import sys
from .prepare import Prepare
from .dump import Dump
sys.path.append("..")
from lib.Loging import Logging
from lib.InitDB import InitMyDB


class ThreadDump(threading.Thread):
    def __init__(self, queue, dump_pro,start_num,end_num,database,table,idx,pri_idx):
        threading.Thread.__init__(self)
        self.queue = queue
        self.dump_pro = dump_pro
        self.start_num = start_num
        self.end_num = end_num
        self.database = database
        self.table = table
        self.idx = idx
        self.pri_idx = pri_idx
    def run(self):
        try:
            __tuple_ = [self.database,self.table,self.idx,self.pri_idx,self.start_num,self.end_num]
            self.dump_pro.dump_to_new_db(*__tuple_)
            self.queue.put('1001')
        except:
            Logging(msg=traceback.format_exc(),level='error')
            return

class processdump(Prepare):
    def __init__(self,threads=None,dbs=None,tables=None,src_kwargs=None,des_kwargs=None,binlog=None):
        super(processdump,self).__init__(threads=threads,src_kwargs=src_kwargs,des_kwargs=des_kwargs)

        self.binlog = binlog
        self.des_kwargs = des_kwargs

        self.databases = dbs
        self.tables = tables
        self.queue = queue.Queue()

        self.des_mysql_conn = None
        self.des_mysql_cur = None
        self.conn,self.cur = None,None
        self.dump = None

        self.__init_info()

    def __init_info(self):
        '''
        初始化数据库主链接信息
        :return:
        '''
        self.des_mysql_conn = InitMyDB(**self.des_kwargs).Init()
        self.des_mysql_cur = self.des_mysql_conn.cursor()
        self.des_thread_list.append({'conn': self.des_mysql_conn, 'cur': self.des_mysql_cur})
        if self.binlog is None:
            self.des_mysql_cur.execute('set sql_log_bin=0')
        self.des_mysql_cur.execute('SET SESSION wait_timeout = 2147483;')
        self.conn, self.cur = self.init_conn(primary_t=True)
        self.dump = Dump(cur=self.cur, des_conn=self.des_mysql_conn, des_cur=self.des_mysql_cur)


    def start(self):
        '''
        所有在线导出操作将在该函数内部直接完成，直至退出并返回binlog相关信息
        binlog信息在所有链接初始化完成后获取，因为所有链接都采用的SNAPSHOT
        因此主链接会执行全局读锁，但非常短暂，在所有链接初始化完成将释放
        :return:
        '''
        binlog_file,binlog_pos = self.master_info(cur=self.cur)
        if binlog_file and binlog_pos:
            pass
        else:
            self.cur.execute('UNLOCK TABLES')
            self.close(self.cur,self.conn)
            Logging(msg='invalid master info , file {} position {}'.format(binlog_file,binlog_pos),level='error')
            sys.exit()

        '''初始化源库、目标库所有链接'''
        if self.threads and self.threads > 1:
            self.init_conn()
            self.init_des_conn(binlog=self.binlog)

        self.cur.execute('UNLOCK TABLES')

        if self.threads and self.threads > 1:
            '''多线程导出'''
            for database in self.databases:
                if self.tables:
                    for tablename in self.tables:
                        _parmeter = [database,tablename]
                        self.__mul_dump_go(*_parmeter)
                        self.__get_queue()
                else:
                    tables = self.get_tables(cur=self.cur, db=database)
                    for tablename in tables:
                        _parmeter = [database, tablename]
                        self.__mul_dump_go(*_parmeter)
                        self.__get_queue()
        else:
            '''单线程导出'''
            for database in self.databases:
                if self.tables:
                    for tablename in self.tables:
                        _parameter = [database,tablename]
                        self.__dump_go(*_parameter)
                else:
                    '''全库导出'''
                    tables = self.get_tables(cur=self.cur,db=database)
                    for table in tables:
                        _parameter = [database, table]
                        self.__dump_go(*_parameter)

        '''操作完成关闭所有数据库链接'''
        if self.threads and self.threads > 1:
            for thread in self.thread_list:
                self.close(thread['cur'],thread['conn'])
            for thread in self.des_thread_list:
                self.close(thread['cur'], thread['conn'])
        return binlog_file,binlog_pos

    def __dump_go(self,database,tablename):
        '''
        单线程导出函数
        :param database:
        :param tablename:
        :return:
        '''
        stat = self.dump.prepare_structe(database=database, tablename=tablename)
        if stat:
            idx_name,pri_idx = self.check_pri(cur=self.cur, db=database, table=tablename)
            self.dump.dump_to_new_db(database=database, tablename=tablename, idx=idx_name,pri_idx=pri_idx)
        else:
            Logging(msg='Initialization structure error', level='error')
            sys.exit()

    def __mul_dump_go(self,database,tablename):
        '''
        多线程导出函数
        通过表总条数按线程数拆分为多个chunk
        :param database:
        :param tablename:
        :return:
        '''
        chunks = self.get_chunks(cur=self.cur, databases=database, tables=tablename)
        stat = self.dump.prepare_structe(database=database, tablename=tablename)
        if stat:
            idx_name,pri_idx = self.check_pri(cur=self.cur, db=database, table=tablename)

            __start_num = 0
            __limit_num = chunks
            for t in range(len(self.thread_list)):
                if len(self.thread_list) - t == 1:
                    __limit_num = None
                dump = Dump(cur=self.thread_list[t]['cur'], des_conn=self.des_thread_list[t]['conn'],
                            des_cur=self.des_thread_list[t]['cur'])
                __dict_ = [self.queue, dump, __start_num, __limit_num, database, tablename, idx_name,pri_idx]
                t = ThreadDump(*__dict_)
                t.start()
                __start_num += chunks
        else:
            Logging(msg='Initialization structure error', level='error')
            sys.exit()

    def __get_queue(self):
        '''
        获取queue中的数据个数，直到取回个数与并发线程数相等才退出
        :return:
        '''
        _count = len(self.thread_list)
        _tmp_count = 0
        while _tmp_count < _count:
            value = self.queue.get()
            if value:
                _tmp_count += 1


