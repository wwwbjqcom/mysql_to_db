# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import datetime
import struct
import sys
import binascii
sys.path.append("..")
from binlog import Metadata,ReadPacket



class ParseEvent(ReadPacket.Read):
    def __init__(self,packet=None,filename=None,startpostion=None,remote=None):
        self.remote = remote
        super(ParseEvent, self).__init__(packet, filename, startpostion)

    def read_header(self):
        '''binlog_event_header_len = 19
        timestamp : 4bytes
        type_code : 1bytes
        server_id : 4bytes
        event_length : 4bytes
        next_position : 4bytes
        flags : 2bytes
        '''
        if self.remote:
            read_byte = self.read_bytes(20)
        else:
            read_byte = self.read_bytes(19)
        if read_byte:
            if self.remote:
                result = struct.unpack('<cIcIIIH', read_byte)
                if isinstance(result[2], int):
                    type_code = result[2]
                else:
                    type_code = struct.unpack("!B", result[2])[0]
                event_length, next_pos = result[4], result[5]
            else:
                result = struct.unpack('=IBIIIH', read_byte)
                type_code, event_length, next_pos = result[1], result[3],result[4]
            return type_code, event_length,next_pos
        else:
            return None, None,None

    def read_query_event(self, event_length=None):
        '''fix_part = 13:
                thread_id : 4bytes
                execute_seconds : 4bytes
                database_length : 1bytes
                error_code : 2bytes
                variable_block_length : 2bytes
            variable_part :
                variable_block_length = fix_part.variable_block_length
                database_name = fix_part.database_length   
                sql_statement = event_header.event_length - 19 - 13 - variable_block_length - database_length - 4
        '''
        read_byte = self.read_bytes(Metadata.binlog_event_fix_part)
        fix_result = struct.unpack('=IIBHH', read_byte)
        thread_id = fix_result[0]
        self.read_bytes(fix_result[4])
        read_byte = self.read_bytes(fix_result[2])
        database_name, = struct.unpack('{}s'.format(fix_result[2]), read_byte)
        statement_length = event_length - Metadata.binlog_event_fix_part - Metadata.binlog_event_header_len \
                           - fix_result[4] - fix_result[2] - Metadata.binlog_quer_event_stern
        read_byte = self.read_bytes(statement_length)
        _a, sql_statement, = struct.unpack('1s{}s'.format(statement_length - 1), read_byte)
        return thread_id, database_name, sql_statement

    def read_rotate_log_event(self,event_length=None):
        '''
        Fixed data part: 8bytes
        Variable data part: event_length - header_length - fixed_length
        :param event_length: 
        :return: 
        '''
        if self.remote:
            variable_length = event_length - Metadata.binlog_event_header_len - 8 - 1 - Metadata.src_length + 1
        else:
            variable_length = event_length - Metadata.binlog_event_header_len - 8 - Metadata.src_length + 1

        self.read_bytes(8)
        value, = struct.unpack('{}s'.format(variable_length),self.read_bytes(variable_length))
        return value


    def read_table_map_event(self, event_length):
        '''
        fix_part = 8
            table_id : 6bytes
            Reserved : 2bytes
        variable_part:
            database_name_length : 1bytes
            database_name : database_name_length bytes + 1
            table_name_length : 1bytes
            table_name : table_name_length bytes + 1
            cloums_count : 1bytes
            colums_type_array : one byte per column  
            mmetadata_lenth : 1bytes
            metadata : .....(only available in the variable length field，varchar:2bytes，text、blob:1bytes,time、timestamp、datetime: 1bytes
                            blob、float、decimal : 1bytes, char、enum、binary、set: 2bytes(column type id :1bytes metadatea: 1bytes))
            bit_filed : 1bytes
            crc : 4bytes
            .........
        :param event_length: 
        :return: 
        '''
        self.read_bytes(Metadata.table_map_event_fix_length)
        database_name_length, = struct.unpack('B', self.read_bytes(1))
        database_name, _a, = struct.unpack('{}ss'.format(database_name_length),
                                           self.read_bytes(database_name_length + 1))
        table_name_length, = struct.unpack('B', self.read_bytes(1))
        table_name, _a, = struct.unpack('{}ss'.format(table_name_length), self.read_bytes(table_name_length + 1))
        colums = self.read_uint8()
        a = '='
        for i in range(colums):
            a += 'B'
        colums_type_id_list = list(struct.unpack(a, self.read_bytes(colums)))
        self.read_bytes(1)
        metadata_dict = {}
        bytes = 1
        for idex in range(len(colums_type_id_list)):
            if colums_type_id_list[idex] in [Metadata.column_type_dict.MYSQL_TYPE_VAR_STRING,
                                             Metadata.column_type_dict.MYSQL_TYPE_VARCHAR]:
                metadata = self.read_uint16()
                metadata_dict[idex] = 2 if metadata > 255 else 1
                bytes += 2
            elif colums_type_id_list[idex] in [Metadata.column_type_dict.MYSQL_TYPE_BLOB,
                                               Metadata.column_type_dict.MYSQL_TYPE_MEDIUM_BLOB,
                                               Metadata.column_type_dict.MYSQL_TYPE_LONG_BLOB,
                                               Metadata.column_type_dict.MYSQL_TYPE_TINY_BLOB,
                                               Metadata.column_type_dict.MYSQL_TYPE_JSON]:
                metadata = self.read_uint8()
                metadata_dict[idex] = metadata
                bytes += 1
            elif colums_type_id_list[idex] in [Metadata.column_type_dict.MYSQL_TYPE_TIMESTAMP2,
                                               Metadata.column_type_dict.MYSQL_TYPE_DATETIME2,
                                               Metadata.column_type_dict.MYSQL_TYPE_TIME2]:
                metadata = self.read_uint8()
                metadata_dict[idex] = metadata
                bytes += 1
            elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_NEWDECIMAL:
                precision = self.read_uint8()
                decimals = self.read_uint8()
                metadata_dict[idex] = [precision, decimals]
                bytes += 2
            elif colums_type_id_list[idex] in [Metadata.column_type_dict.MYSQL_TYPE_FLOAT, Metadata.column_type_dict.MYSQL_TYPE_DOUBLE]:
                metadata = self.read_uint8()
                metadata_dict[idex] = metadata
                bytes += 1
            elif colums_type_id_list[idex] in [Metadata.column_type_dict.MYSQL_TYPE_STRING]:
                _type, metadata, = struct.unpack('=BB', self.read_bytes(2))
                if colums_type_id_list[idex] != _type:
                    metadata_dict[idex] = 65535
                else:
                    metadata_dict[idex] = metadata
                bytes += 2

        if self.packet is None:
            self.file_data.seek(
                event_length - Metadata.binlog_event_header_len - Metadata.table_map_event_fix_length - 5 - database_name_length
                - table_name_length - colums - bytes, 1)
        return database_name, table_name, colums_type_id_list, metadata_dict

    def read_gtid_event(self, event_length=None):
        '''
        The layout of the buffer is as follows:
        +------+--------+-------+-------+--------------+---------------+
        |flags |SID     |GNO    |lt_type|last_committed|sequence_number|
        |1 byte|16 bytes|8 bytes|1 byte |8 bytes       |8 bytes        |
        +------+--------+-------+-------+--------------+---------------+

        The 'flags' field contains gtid flags.
            0 : rbr_only ,"/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*/%s\n"
            1 : sbr

        lt_type (for logical timestamp typecode) is always equal to the
        constant LOGICAL_TIMESTAMP_TYPECODE.

        5.6 did not have TS_TYPE and the following fields. 5.7.4 and
        earlier had a different value for TS_TYPE and a shorter length for
        the following fields. Both these cases are accepted and ignored.

        The buffer is advanced in Binary_log_event constructor to point to
        beginning of post-header
        :param event_length: 
        :return: 
        '''
        self.read_bytes(1)
        uuid = self.read_bytes(16)
        nibbles = binascii.hexlify(uuid).decode('ascii')
        gtid = '%s-%s-%s-%s-%s' % (nibbles[:8], nibbles[8:12], nibbles[12:16], nibbles[16:20], nibbles[20:])
        gno_id = self.read_uint64()
        gtid += ":{}".format(gno_id)
        if self.packet is None:
            self.file_data.seek(event_length - 1 - 16 - 8 - Metadata.binlog_event_header_len, 1)
        return gtid

    def read_xid_variable(self):
        xid_num = self.read_uint64()
        return xid_num

    def read_decode(self, count):
        _value = self.read_bytes(count)
        return struct.unpack('{}s'.format(count), _value)[0]

    def read_row_event(self, event_length, colums_type_id_list, metadata_dict, type,unsigned_list):
        '''
        fixed_part: 10bytes
            table_id: 6bytes
            reserved: 2bytes
            extra: 2bytes
        variable_part:
            columns: 1bytes
            variable_sized: int((n+7)/8) n=columns.value
            variable_sized: int((n+7)/8) (for updata_row_event only)

            variable_sized: int((n+7)/8)
            row_value : variable size

            crc : 4bytes

        The The data first length of the varchar type more than 255 are 2 bytes
        '''
        self.read_bytes(Metadata.fix_length + Metadata.binlog_row_event_extra_headers)
        columns = self.read_uint8()
        columns_length = int((columns + 7) / 8)
        self.read_bytes(columns_length)
        if type == Metadata.binlog_events.UPDATE_ROWS_EVENT:
            self.read_bytes(columns_length)
            bytes = Metadata.binlog_event_header_len + Metadata.fix_length + Metadata.binlog_row_event_extra_headers + 1 + columns_length + columns_length
        else:
            bytes = Metadata.binlog_event_header_len + Metadata.fix_length + Metadata.binlog_row_event_extra_headers + 1 + columns_length
        __values = []
        while event_length - bytes > Metadata.binlog_quer_event_stern:
            values = []
            null_bit = self.read_bytes(columns_length)
            bytes += columns_length
            for idex in range(len(colums_type_id_list)):
                if self.is_null(null_bit, idex):
                    values.append(None)
                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_TINY:
                    if 'unsigned' in unsigned_list[idex]:
                        values.append(self.read_uint8())
                    else:
                        values.append(self.read_int8())
                    bytes += 1
                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_SHORT:
                    if 'unsigned' in unsigned_list[idex]:
                        values.append(self.read_uint16())
                    else:
                        values.append(self.read_int16())
                    bytes += 2
                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_INT24:
                    if 'unsigned' in unsigned_list[idex]:
                        values.append(self.read_uint24())
                    else:
                        values.append(self.read_int24())
                    bytes += 3
                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_LONG:
                    if 'unsigned' in unsigned_list[idex]:
                        values.append(self.read_uint32())
                    else:
                        values.append(self.read_int32())
                    bytes += 4
                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_LONGLONG:
                    if 'unsigned' in unsigned_list[idex]:
                        values.append(self.read_uint64())
                    else:
                        values.append(self.read_int64())
                    bytes += 8

                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_NEWDECIMAL:
                    _list = metadata_dict[idex]
                    decimals, read_bytes = self.read_new_decimal(precision=_list[0], decimals=_list[1])
                    values.append(decimals)
                    bytes += read_bytes

                elif colums_type_id_list[idex] in [Metadata.column_type_dict.MYSQL_TYPE_DOUBLE,
                                                   Metadata.column_type_dict.MYSQL_TYPE_FLOAT]:
                    _read_bytes = metadata_dict[idex]
                    if _read_bytes == 8:
                        _values, = struct.unpack('<d', self.read_bytes(_read_bytes))
                    elif _read_bytes == 4:
                        _values, = struct.unpack('<f', self.read_bytes(4))
                    values.append(_values)
                    bytes += _read_bytes

                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_TIMESTAMP2:
                    _time, read_bytes = self.add_fsp_to_time(datetime.datetime.fromtimestamp(self.read_int_be_by_size(4)), metadata_dict[idex])
                    values.append(str(_time))
                    bytes += read_bytes + 4
                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_DATETIME2:
                    _time, read_bytes = self.read_datetime2(metadata_dict[idex])
                    values.append(str(_time))
                    bytes += 5 + read_bytes
                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_YEAR:
                    _date = self.read_uint8() + 1900
                    values.append(_date)
                    bytes += 1
                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_DATE:
                    _time = self.read_date()
                    values.append(str(_time))
                    bytes += 3

                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_TIME2:
                    _time, read_bytes = self.read_time2(metadata_dict[idex])
                    bytes += read_bytes
                    values.append(str(_time))

                elif colums_type_id_list[idex] in [Metadata.column_type_dict.MYSQL_TYPE_VARCHAR,
                                                   Metadata.column_type_dict.MYSQL_TYPE_VAR_STRING,
                                                   Metadata.column_type_dict.MYSQL_TYPE_BLOB,
                                                   Metadata.column_type_dict.MYSQL_TYPE_TINY_BLOB,
                                                   Metadata.column_type_dict.MYSQL_TYPE_LONG_BLOB,
                                                   Metadata.column_type_dict.MYSQL_TYPE_MEDIUM_BLOB,
                                                   Metadata.column_type_dict.MYSQL_TYPE_SET,
                                                   Metadata.column_type_dict.MYSQL_TYPE_BIT]:
                    _metadata = metadata_dict[idex]
                    value_length = self.read_uint_by_size(_metadata)
                    values.append(self.read_decode(value_length))
                    bytes += value_length + _metadata
                elif colums_type_id_list[idex] in [Metadata.column_type_dict.MYSQL_TYPE_JSON]:
                    _metadata = metadata_dict[idex]
                    value_length = self.read_uint_by_size(_metadata)
                    values.append(str(self.read_binary_json(value_length)))
                    bytes += value_length + _metadata
                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_STRING:
                    _metadata = metadata_dict[idex]
                    if _metadata <= 255:
                        value_length = self.read_uint8()
                        values.append(self.read_decode(value_length))
                        _read = 1
                    else:
                        value_length = self.read_uint16()
                        values.append(self.read_decode(value_length))
                        _read = 2
                    bytes += value_length + _read
                elif colums_type_id_list[idex] == Metadata.column_type_dict.MYSQL_TYPE_ENUM:
                    _metadata = metadata_dict[idex]
                    if _metadata == 1:
                        values.append(self.read_uint8())
                    elif _metadata == 2:
                        values.append(self.read_uint16())
                    bytes += _metadata

            #if type == Metadata.binlog_events.UPDATE_ROWS_EVENT:
            __values.append(values)

            #else:
        return __values

    def write_row_event(self, event_length, colums_type_id_list, metadata_dict, type):
        __values = self.read_row_event(event_length, colums_type_id_list, metadata_dict, type)

    def delete_row_event(self, event_length, colums_type_id_list, metadata_dict, type):
        __values = self.read_row_event(event_length, colums_type_id_list, metadata_dict, type)

    def update_row_event(self, event_length, colums_type_id_list, metadata_dict, type):
        values = self.read_row_event(event_length, colums_type_id_list, metadata_dict, type)
        __values = [values[i:i + 2] for i in range(0, len(values), 2)]

    def row_event(self,event_length, colums_type_id_list, metadata_dict, type):
        pass

    def GetValue(self,cloums_type_id_list=None, metadata_dict=None,type_code=None,event_length=None,unsigned_list=None):
        database_name, table_name,values = None,None,[]

        if type_code == Metadata.binlog_events.TABLE_MAP_EVENT:
            database_name, table_name, cloums_type_id_list, metadata_dict = self.read_table_map_event(
                event_length)
            return database_name.decode('utf-8'), table_name.decode('utf-8'), cloums_type_id_list, metadata_dict
        elif type_code in (Metadata.binlog_events.WRITE_ROWS_EVENT,Metadata.binlog_events.DELETE_ROWS_EVENT,
                           Metadata.binlog_events.UPDATE_ROWS_EVENT):
            values = self.read_row_event(event_length=event_length,colums_type_id_list=cloums_type_id_list,
                                         metadata_dict=metadata_dict,type=type_code,unsigned_list=unsigned_list)
            return values
        elif type_code == Metadata.binlog_events.UPDATE_ROWS_EVENT:
            pass
        elif type_code == Metadata.binlog_events.GTID_LOG_EVENT:
            pass
        elif type_code == Metadata.binlog_events.QUERY_EVENT:
            pass
        elif type_code == Metadata.binlog_events.XID_EVENT:
            pass




#ParseEvent(packet='',filename='',startpostion='').read_header()


