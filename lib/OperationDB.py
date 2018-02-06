# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import sys,pymysql,traceback
from Loging import Logging
from InitDB import InitMyDB
sys.path.append("..")
from binlog.Replication import ReplicationMysql
from binlog.ParseEvent import ParseEvent
from binlog.PrepareStructure import GetStruct
from binlog.Metadata import binlog_events
from binlog.Metadata import column_type_dict

class tmepdata:
    database_name,table_name,cloums_type_id_list,metadata_dict = None,None,None,None
    table_struct_list = {}              #字段名列表
    table_pk_idex_list = {}             #主键索引列表
    table_struct_type_list = {}         #字段类型列表
    rollback_sql_list = []
    transaction_sql_list = []

class OperationDB:
    def __init__(self,**kwargs):
        self.host,self.port,self.user,self.passwd = kwargs['host'],kwargs['port'],kwargs['user'],kwargs['passwd']
        self.unix_socket = kwargs['socket']
        self.dhost,self.dport,self.duser,self.dpasswd = kwargs['dhost'],kwargs['dport'],kwargs['duser'],kwargs['dpasswd']
        self.databases = kwargs['databases']
        self.tables = kwargs['tables']
        self.binlog_file = kwargs['binlog_file']
        self.start_position = kwargs['start_position']
        self.conn = InitMyDB(mysql_host=self.host, mysql_port=self.port, mysql_user=self.user,
                              mysql_password=self.passwd, unix_scoket=self.unix_socket).Init()

        self.destination_conn = InitMyDB(mysql_host=self.dhost, mysql_port=self.dport, mysql_user=self.duser,
                                    mysql_password=self.dpasswd).Init()

        self.destination_cur = self.destination_conn.cursor()



    def WhereJoin(self,values,table_struce_key):
        __tmp = []
        for idex,col in enumerate(tmepdata.table_struct_list[table_struce_key]):
            if tmepdata.cloums_type_id_list[idex] not in (column_type_dict.MYSQL_TYPE_LONGLONG,column_type_dict.MYSQL_TYPE_LONG,column_type_dict.MYSQL_TYPE_SHORT,column_type_dict.MYSQL_TYPE_TINY,column_type_dict.MYSQL_TYPE_INT24):
                if values[idex] in ('Null','null'):
                    __tmp.append('{} is null'.format(col))
                else:
                    __tmp.append('{}="{}"'.format(col, values[idex]))
            else:
                if values[idex] in ('Null','null'):
                    __tmp.append('{} is null'.format(col))
                else:
                    __tmp.append('{}={}'.format(col,values[idex]))
        return 'AND'.join(__tmp)

    def SetJoin(self,values,table_struce_key):
        __tmp = []
        for idex, col in enumerate(tmepdata.table_struct_list[table_struce_key]):
            if tmepdata.cloums_type_id_list[idex] not in (
            column_type_dict.MYSQL_TYPE_LONGLONG, column_type_dict.MYSQL_TYPE_LONG, column_type_dict.MYSQL_TYPE_SHORT,
            column_type_dict.MYSQL_TYPE_TINY, column_type_dict.MYSQL_TYPE_INT24):
                if values[idex] in ('Null','null'):
                    __tmp.append('{}=null'.format(col))
                else:
                    table_column_type = tmepdata.table_struct_type_list[table_struce_key][idex]
                    if 'blob' not in table_column_type and 'set' not in table_column_type:
                        __tmp.append('{}="{}"'.format(col, values[idex]))
                    else:
                        __tmp.append('{}={}'.format(col, pymysql.Binary(values[idex])))
            else:
                if values[idex] in ('Null','null'):
                    __tmp.append('{}=null'.format(col))
                else:
                    __tmp.append('{}={}'.format(col, values[idex]))
        return ','.join(__tmp)

    def ValueJoin(self,values, table_struce_key):
        __tmp = '('
        for idex, col in enumerate(tmepdata.table_struct_list[table_struce_key]):
            if tmepdata.cloums_type_id_list[idex] in (
                    column_type_dict.MYSQL_TYPE_LONGLONG, column_type_dict.MYSQL_TYPE_LONG,
                    column_type_dict.MYSQL_TYPE_SHORT,
                    column_type_dict.MYSQL_TYPE_TINY, column_type_dict.MYSQL_TYPE_INT24):
                if idex < len(values) - 1:
                    __tmp += '{},'.format(values[idex])
                else:
                    __tmp += '{})'.format(values[idex])
            else:
                if values[idex] in ('Null','null'):
                    if idex < len(values) - 1:
                        __tmp += 'Null,'
                    else:
                        __tmp += 'Null)'
                else:
                    table_column_type = tmepdata.table_struct_type_list[table_struce_key][idex]
                    if idex < len(values) - 1:
                        if 'blob' not in table_column_type and 'set' not in table_column_type:
                            __tmp += '"{}",'.format(values[idex])
                        else:
                            __tmp += '{}'.format(pymysql.Binary(values[idex]))
                    else:
                        if 'blob' not in table_column_type and 'set' not in table_column_type:
                            __tmp += '"{}")'.format(values[idex])
                        else:
                            __tmp += '{}'.format(pymysql.Binary(values[idex]))
        return __tmp

    def GetSQL(self,_values=None,event_code=None):
        table_struce_key = '{}:{}'.format(tmepdata.database_name,tmepdata.table_name)
        if table_struce_key not in tmepdata.table_struct_list:
            column_list, pk_idex,column_type_list = GetStruct(host=self.host,port=self.port,user=self.user,passwd=self.passwd).GetColumn(tmepdata.database_name,tmepdata.table_name)
            tmepdata.table_struct_list[table_struce_key] = column_list
            tmepdata.table_pk_idex_list[table_struce_key] = pk_idex
            tmepdata.table_struct_type_list[table_struce_key] = column_type_list

        if table_struce_key in tmepdata.table_pk_idex_list:
            '''获取主键所在index'''
            __pk_idx = tmepdata.table_pk_idex_list[table_struce_key]
            pk = tmepdata.table_struct_list[table_struce_key][__pk_idx]
        else:
            __pk_idx = None

        if event_code == binlog_events.UPDATE_ROWS_EVENT:
            __values = [_values[i:i + 2] for i in xrange(0, len(_values), 2)]
            for row_value in __values:
                if __pk_idx is not None:
                    roll_pk_value, cur_pk_value =  row_value[1][__pk_idx], row_value[0][__pk_idx]
                    cur_sql = 'UPDATE {}.{} SET {} WHERE {}={}'.format(tmepdata.database_name, tmepdata.table_name,
                                                                       self.SetJoin(row_value[1], table_struce_key), pk,
                                                                       cur_pk_value)
                else:
                    cur_sql = 'UPDATE {}.{} SET {} WHERE {}'.format(tmepdata.database_name, tmepdata.table_name,
                                                                   self.SetJoin(row_value[1], table_struce_key),
                                                                   self.WhereJoin(row_value[0], table_struce_key))
                tmepdata.transaction_sql_list.append(cur_sql)
        else:
            for value in _values:
                '''获取sql语句'''
                if event_code == binlog_events.WRITE_ROWS_EVENT:
                    if len(value) > 1:
                        cur_sql = 'INSERT INTO {}.{} VALUES{};'.format(tmepdata.database_name, tmepdata.table_name,
                                                                       self.ValueJoin(value,table_struce_key))
                    else:
                        if isinstance(value[0], int):
                            cur_sql = 'INSERT INTO {}.{} VALUES({});'.format(tmepdata.database_name, tmepdata.table_name,
                                                                             value[0])
                        else:
                            cur_sql = 'INSERT INTO {}.{} VALUES("{}");'.format(tmepdata.database_name,
                                                                               tmepdata.table_name,
                                                                               value[0])

                    tmepdata.transaction_sql_list.append(cur_sql)
                elif event_code == binlog_events.DELETE_ROWS_EVENT:
                    if __pk_idx is not None:
                        cur_sql = 'DELETE FROM {}.{} WHERE {}={};'.format(tmepdata.database_name,tmepdata.table_name,pk,value[__pk_idx])
                    else:
                        cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name,tmepdata.table_name,self.WhereJoin(value,table_struce_key))
                    tmepdata.transaction_sql_list.append(cur_sql)



    def Operation(self):
        Logging(msg='replication to master.............', level='info')
        ReplConn = ReplicationMysql(log_file=self.binlog_file, log_pos=self.start_position,mysql_connection=self.conn).ReadPack()
        event_length = None
        if ReplConn:
            Logging(msg='replication succeed................', level='info')
            while True:
                try:
                    if pymysql.__version__ < "0.6":
                        pkt = ReplConn.read_packet()
                    else:
                        pkt = ReplConn._read_packet()
                    at_pos = self.start_position + event_length if event_length else self.start_position
                    _parse_event = ParseEvent(packet=pkt,remote=True)
                    event_code, event_length = _parse_event.read_header()
                    if event_code is None:
                        continue
                    next_pos = at_pos + event_length
                    if event_code in (binlog_events.WRITE_ROWS_EVENT,binlog_events.UPDATE_ROWS_EVENT,binlog_events.DELETE_ROWS_EVENT):
                        if tmepdata.database_name and tmepdata.table_name and tmepdata.database_name in self.databases:
                            if self.tables:
                                if tmepdata.table_name in self.tables:
                                    _values = _parse_event.GetValue(type_code=event_code, event_length=event_length,cloums_type_id_list=tmepdata.cloums_type_id_list,metadata_dict=tmepdata.metadata_dict)
                                    self.GetSQL(_values=_values, event_code=event_code)
                                    state = self.__put_new_db()
                                    if state is None:
                                        Logging(msg='failed!!!!', level='error')
                                        break
                            else:
                                _values = _parse_event.GetValue(type_code=event_code, event_length=event_length,
                                                                cloums_type_id_list=tmepdata.cloums_type_id_list,
                                                                metadata_dict=tmepdata.metadata_dict)
                                self.GetSQL(_values=_values, event_code=event_code)
                                state = self.__put_new_db()
                                if state is None:
                                    Logging(msg='failed!!!!', level='error')
                                    break
                            tmepdata.transaction_sql_list = []
                    elif event_code == binlog_events.TABLE_MAP_EVENT:
                        tmepdata.database_name, tmepdata.table_name, tmepdata.cloums_type_id_list, tmepdata.metadata_dict=_parse_event.GetValue(type_code=event_code,event_length=event_length)  # 获取event数据

                except Exception,e:
                    Logging(msg=traceback.format_exc(),level='error')
                    ReplConn.close()
                    break
                Logging(msg='execute binlog position : {}, next position: {}'.format(at_pos,next_pos), level='info')
        else:
            Logging(msg='replication failed................', level='error')

    def __put_new_db(self):

        for sql in tmepdata.transaction_sql_list:
            try:
                self.destination_cur.execute(sql)
            except pymysql.ERROR,e:
				Logging(msg=traceback.format_exc(),level='error')
                self.destination_conn.rollback()
                return None
        self.destination_conn.commit()
        return True




