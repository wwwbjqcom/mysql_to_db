# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import sys,pymysql,traceback
from .Loging import Logging
from .InitDB import InitMyDB
sys.path.append("..")
from dump.processdump import processdump
from binlog.Replication import ReplicationMysql
from binlog.ParseEvent import ParseEvent
from binlog.PrepareStructure import GetStruct
from binlog.Metadata import binlog_events

class tmepdata:
    database_name,table_name,cloums_type_id_list,metadata_dict = None,None,None,None
    table_struct_list = {}              #字段名列表
    table_pk_idex_list = {}             #主键索引列表
    table_struct_type_list = {}         #字段类型列表
    unsigned_list = {}
    thread_id = None

class OperationDB:
    def __init__(self,**kwargs):
        self.full_dump = kwargs['full_dump']                                                                                                        #是否全量导出
        self.threads = kwargs['threads']                                                                                                            #全量导出时并发线程

        self.host,self.port,self.user,self.passwd = kwargs['host'],kwargs['port'],kwargs['user'],kwargs['passwd']                                   #源库连接相关信息
        self.unix_socket = kwargs['socket']

        self.dhost,self.dport,self.duser,self.dpasswd = kwargs['dhost'],kwargs['dport'],kwargs['duser'],kwargs['dpasswd']                           #目标库连接相关信息
        self.binlog = kwargs['binlog']                                                                                                              #是否在目标库记录binlog的参数

        '''目标库连接'''
        self.destination_conn = InitMyDB(mysql_host=self.dhost, mysql_port=self.dport, mysql_user=self.duser,
                                         mysql_password=self.dpasswd).Init()

        self.destination_cur = self.destination_conn.cursor()
        if self.binlog is None:
            self.destination_cur.execute('set sql_log_bin=0;')  # 设置binlog参数
        self.destination_cur.execute('SET SESSION wait_timeout = 2147483;')
        ''''''

        self.databases = kwargs['databases']
        self.tables = kwargs['tables']
        self.binlog_file = kwargs['binlog_file']
        self.start_position = kwargs['start_position']
        self.conn = InitMyDB(mysql_host=self.host, mysql_port=self.port, mysql_user=self.user,
                              mysql_password=self.passwd, unix_socket=self.unix_socket).Init()

        self.ithread = kwargs['ithread']
        self.ignore_type = kwargs['ignore_type']
        self.ignore = {'delete':binlog_events.DELETE_ROWS_EVENT,'update':binlog_events.UPDATE_ROWS_EVENT,'insert':binlog_events.WRITE_ROWS_EVENT}
        self.server_id = kwargs['server_id']

    def WhereJoin(self,table_struce_key):
        return ' AND '.join(['`{}`=%s'.format(col) for col in tmepdata.table_struct_list[table_struce_key]])

    def SetJoin(self,table_struce_key):
        return ','.join(['`{}`=%s'.format(col) for col in tmepdata.table_struct_list[table_struce_key]])

    def ValueJoin(self,table_struce_key):
        return '({})'.format(','.join(['%s' for i in range(len(tmepdata.table_struct_list[table_struce_key]))]))

    def PkJoin(self,pk_list,table_struce_key):
        pk_col = []
        for pk in pk_list:
            pk_col.append(tmepdata.table_struct_list[table_struce_key][pk])
        if len(pk_col) > 1:
            return ' AND '.join(['`{}`=%s'.format(col) for col in pk_col])
        else:
            return '`{}`=%s'.format(pk_col[0])

    def GetSQL(self,_values=None,event_code=None):
        table_struce_key = '{}:{}'.format(tmepdata.database_name,tmepdata.table_name)

        if tmepdata.table_pk_idex_list[table_struce_key]:
            '''获取主键所在index'''
            __pk_idx = tmepdata.table_pk_idex_list[table_struce_key]
            pk_where = self.PkJoin(__pk_idx,table_struce_key)
        else:
            __pk_idx = None

        if event_code == binlog_events.UPDATE_ROWS_EVENT:
            __values = [_values[i:i + 2] for i in range(0, len(_values), 2)]
            for row_value in __values:
                if __pk_idx is not None:
                    cur_sql = 'UPDATE {}.{} SET {} WHERE {}'.format(tmepdata.database_name, tmepdata.table_name,
                                                                       self.SetJoin(table_struce_key), pk_where)
                    pk_values = []
                    for i in __pk_idx:
                        pk_values.append(row_value[0][i])
                    _args = row_value[1] + pk_values
                else:
                    cur_sql = 'UPDATE {}.{} SET {} WHERE {}'.format(tmepdata.database_name, tmepdata.table_name,
                                                                   self.SetJoin(table_struce_key),
                                                                   self.WhereJoin(table_struce_key))
                    _args = row_value[1] + row_value[0]
                state = self.__put_new_db(cur_sql,args=_args)
        else:
            for value in _values:
                '''获取sql语句'''
                if event_code == binlog_events.WRITE_ROWS_EVENT:
                    cur_sql = 'INSERT INTO {}.{} VALUES{};'.format(tmepdata.database_name, tmepdata.table_name,
                                                                   self.ValueJoin(table_struce_key))
                    _args = value

                elif event_code == binlog_events.DELETE_ROWS_EVENT:
                    if __pk_idx is not None:
                        cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name,tmepdata.table_name,pk_where)
                        pk_values = []
                        for i in __pk_idx:
                            pk_values.append(value[i])
                        _args = pk_values
                    else:
                        cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name,tmepdata.table_name,self.WhereJoin(table_struce_key))
                        _args = value
                state = self.__put_new_db(cur_sql,args=_args)

        if state:
            self.destination_conn.commit()
        else:
            self.destination_conn.rollback()
            Logging(msg='failed!!!!', level='error')
            sys.exit()

    def __execute_code(self,_parse_event,event_code,event_length,table_struce_key):
        if tmepdata.database_name and tmepdata.table_name and tmepdata.database_name in self.databases:
            if self.tables:
                if tmepdata.table_name in self.tables:
                    _values = _parse_event.GetValue(type_code=event_code, event_length=event_length,
                                                    cloums_type_id_list=tmepdata.cloums_type_id_list,
                                                    metadata_dict=tmepdata.metadata_dict,
                                                    unsigned_list=tmepdata.table_struct_type_list[table_struce_key])
                    self.GetSQL(_values=_values, event_code=event_code)
            else:
                _values = _parse_event.GetValue(type_code=event_code, event_length=event_length,
                                                cloums_type_id_list=tmepdata.cloums_type_id_list,
                                                metadata_dict=tmepdata.metadata_dict,
                                                unsigned_list=tmepdata.table_struct_type_list[table_struce_key])
                self.GetSQL(_values=_values, event_code=event_code)

    def Operation(self):
        '''全量导出入口'''
        if self.full_dump:
            des_mysql_info = {'mysql_host':self.dhost,'mysql_port':self.dport,'mysql_user':self.duser,'mysql_password':self.dpasswd}
            src_mysql_info = {'mysql_host':self.host,'mysql_port':self.port,'mysql_user':self.user,'mysql_password':self.passwd,'unix_socket':self.unix_socket}
            _binlog_file,_binlog_pos = processdump(threads=self.threads,dbs=self.databases,tables=self.tables,src_kwargs=src_mysql_info,des_kwargs=des_mysql_info,binlog=self.binlog).start()
            if _binlog_file is None or _binlog_pos is None:
                sys.exit()
        ''''''
        Logging(msg='replication to master.............', level='info')
        if self.full_dump:
            ReplConn = ReplicationMysql(log_file=_binlog_file, log_pos=_binlog_pos,mysql_connection=self.conn, server_id=self.server_id).ReadPack()
        else:
            ReplConn = ReplicationMysql(log_file=self.binlog_file, log_pos=self.start_position,mysql_connection=self.conn,server_id=self.server_id).ReadPack()
        table_struce_key = None
        next_pos = None
        binlog_file_name = self.binlog_file

        _mysql_conn = GetStruct(host=self.host, port=self.port,user=self.user,passwd=self.passwd)
        _mysql_conn.CreateTmp()
        if ReplConn:
            Logging(msg='replication succeed................', level='info')
            at_pos = self.start_position
            while 1:
                try:
                    pkt = ReplConn._read_packet()
                    _parse_event = ParseEvent(packet=pkt,remote=True)
                    event_code, event_length ,next_pos= _parse_event.read_header()
                    if event_code is None:
                        continue
                    if event_code in (binlog_events.WRITE_ROWS_EVENT,binlog_events.UPDATE_ROWS_EVENT,binlog_events.DELETE_ROWS_EVENT):
                        if self.ithread:
                            if self.ithread == tmepdata.thread_id:
                                continue
                            if self.ignore_type and self.ignore[self.ignore_type] == event_code:
                                continue
                            self.__execute_code(_parse_event=_parse_event,event_code=event_code,
                                                    event_length=event_length,table_struce_key=table_struce_key)
                        else:
                            if self.ignore_type and self.ignore[self.ignore_type] == event_code:
                                continue
                            self.__execute_code(_parse_event=_parse_event, event_code=event_code,
                                                    event_length=event_length, table_struce_key=table_struce_key)

                    elif event_code == binlog_events.TABLE_MAP_EVENT:
                        tmepdata.database_name, tmepdata.table_name, tmepdata.cloums_type_id_list, tmepdata.metadata_dict=_parse_event.GetValue(type_code=event_code,event_length=event_length)  # 获取event数据
                        table_struce_key = '{}:{}'.format(tmepdata.database_name, tmepdata.table_name)
                        if table_struce_key not in tmepdata.table_struct_list:
                            if tmepdata.database_name in self.databases:
                                if self.tables:
                                    if tmepdata.table_name in self.tables:
                                        column_list, pk_idex, column_type_list = _mysql_conn.GetColumn(tmepdata.database_name, tmepdata.table_name)
                                        tmepdata.table_struct_list[table_struce_key] = column_list
                                        tmepdata.table_pk_idex_list[table_struce_key] = pk_idex
                                        tmepdata.table_struct_type_list[table_struce_key] = column_type_list
                                    continue
                                column_list, pk_idex, column_type_list = _mysql_conn.GetColumn(tmepdata.database_name, tmepdata.table_name)
                                tmepdata.table_struct_list[table_struce_key] = column_list
                                tmepdata.table_pk_idex_list[table_struce_key] = pk_idex
                                tmepdata.table_struct_type_list[table_struce_key] = column_type_list
                    elif event_code == binlog_events.ROTATE_EVENT:
                            binlog_file_name = _parse_event.read_rotate_log_event(event_length=event_length)
                    elif event_code == binlog_events.QUERY_EVENT:
                        if self.ithread:
                            tmepdata.thread_id,_,_ = _parse_event.read_query_event(event_length=event_length)
                except:
                    Logging(msg=traceback.format_exc(),level='error')
                    ReplConn.close()
                    break
                _mysql_conn.SaveStatus(logname=binlog_file_name,at_pos=at_pos,next_pos=next_pos,server_id=self.server_id)
                at_pos = next_pos
        else:
            Logging(msg='replication failed................', level='error')
            _mysql_conn.close()

    def __put_new_db(self,sql,args):

        try:
            self.destination_cur.execute(sql,args)
        except pymysql.Error:
            Logging(msg=[sql,args],level='error')
            Logging(msg=traceback.format_exc(),level='error')
            return None
        return True




