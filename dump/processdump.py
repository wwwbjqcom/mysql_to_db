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
    def __init__(self, queue, dump_pro,start_num,end_num,database,table,idx):
        threading.Thread.__init__(self)
        self.queue = queue
        self.dump_pro = dump_pro
        self.start_num = start_num
        self.end_num = end_num
        self.database = database
        self.table = table
        self.idx = idx
    def run(self):
        try:
            __tuple_ = [self.database,self.table,self.idx,self.start_num,self.end_num]
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
        self.des_mysql_conn = InitMyDB(**self.des_kwargs).Init()
        self.des_mysql_cur = self.des_mysql_conn.cursor()  # 目标库连接
        if self.binlog is None:
            self.des_mysql_cur.execute('set sql_log_bin=0')
        self.des_mysql_cur.execute('SET SESSION wait_timeout = 2147483;')

        self.conn, self.cur = self.init_conn(primary_t=True)  # 初始化主连接
        self.dump = Dump(cur=self.cur, des_conn=self.des_mysql_conn, des_cur=self.des_mysql_cur)


    def start(self):
        binlog_file,binlog_pos = self.master_info(cur=self.cur)
        if binlog_file and binlog_pos:
            pass
        else:
            self.cur.execute('UNLOCK TABLES')
            self.close(self.cur,self.conn)
            Logging(msg='invalid master info , file {} position {}'.format(binlog_file,binlog_pos),level='error')
            sys.exit()

        if self.threads and self.threads > 1:
            self.init_conn()    #初始化所有线程连接
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
        self.close(self.cur,self.conn)
        if self.threads and self.threads > 1:
            for thread in self.thread_list:
                self.close(thread['cur'],thread['conn'])
            for thread in self.des_thread_list:
                self.close(thread['cur'], thread['conn'])
        return binlog_file,binlog_pos

    def __dump_go(self,database,tablename):
        '''每个表开始操作时重新初始化基础链接'''
        try:
            self.close(self.cur, self.conn)
            self.close(self.des_mysql_cur, self.des_mysql_conn)
            self.__init_info()
        except:
            pass
        stat = self.dump.prepare_structe(database=database, tablename=tablename)
        if stat:
            idx_name = self.check_pri(cur=self.cur, db=database, table=tablename)
            self.dump.dump_to_new_db(database=database, tablename=tablename, idx=idx_name)
        else:
            Logging(msg='Initialization structure error', level='error')
            sys.exit()

    def __mul_dump_go(self,database,tablename):
        chunks = self.get_chunks(cur=self.cur, databases=database, tables=tablename)
        if chunks is None:
            '''在对表初始化时失败将重新初始化基础链接'''
            try:
                self.close(self.cur,self.conn)
                self.close(self.des_mysql_cur,self.des_mysql_conn)
            except:
                pass
            self.__init_info()
        stat = self.dump.prepare_structe(database=database, tablename=tablename)
        if stat:
            idx_name = self.check_pri(cur=self.cur, db=database, table=tablename)

            __start_num = 0
            __limit_num = chunks
            for t in range(len(self.thread_list)):
                '''多线程导出每个线程对mysql链接及基本信息'''
                if len(self.thread_list) - t == 1:
                    __limit_num = None
                dump = Dump(cur=self.thread_list[t]['cur'], des_conn=self.des_thread_list[t]['conn'],
                            des_cur=self.des_thread_list[t]['cur'])
                __dict_ = [self.queue, dump, __start_num, __limit_num, database, tablename, idx_name]
                t = ThreadDump(*__dict_)
                t.start()
                __start_num += chunks
        else:
            Logging(msg='Initialization structure error', level='error')
            sys.exit()

    def __get_queue(self):
        _count = len(self.thread_list)
        _tmp_count = 0
        while _tmp_count < _count:
            value = self.queue.get()
            if value:
                _tmp_count += 1


