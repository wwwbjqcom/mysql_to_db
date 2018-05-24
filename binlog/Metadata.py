# -*- encoding: utf-8 -*-
'''
@author: Great God
'''

class column_type_dict:
    MYSQL_TYPE_DECIMAL=0
    MYSQL_TYPE_TINY=1
    MYSQL_TYPE_SHORT=2
    MYSQL_TYPE_LONG=3
    MYSQL_TYPE_FLOAT=4
    MYSQL_TYPE_DOUBLE=5
    MYSQL_TYPE_NULL=6
    MYSQL_TYPE_TIMESTAMP=7
    MYSQL_TYPE_LONGLONG=8
    MYSQL_TYPE_INT24=9
    MYSQL_TYPE_DATE=10
    MYSQL_TYPE_TIME=11
    MYSQL_TYPE_DATETIME=12
    MYSQL_TYPE_YEAR=13
    MYSQL_TYPE_NEWDATE=14
    MYSQL_TYPE_VARCHAR=15
    MYSQL_TYPE_BIT=16
    MYSQL_TYPE_TIMESTAMP2=17
    MYSQL_TYPE_DATETIME2=18
    MYSQL_TYPE_TIME2=19
    MYSQL_TYPE_JSON=245
    MYSQL_TYPE_NEWDECIMAL=246
    MYSQL_TYPE_ENUM=247
    MYSQL_TYPE_SET=248
    MYSQL_TYPE_TINY_BLOB=249
    MYSQL_TYPE_MEDIUM_BLOB=250
    MYSQL_TYPE_LONG_BLOB=251
    MYSQL_TYPE_BLOB=252
    MYSQL_TYPE_VAR_STRING=253
    MYSQL_TYPE_STRING=254
    MYSQL_TYPE_GEOMETRY=255

''''''

'''mysql_binlog_event start GTID_EVENT end XID_EVENT'''
class binlog_events:
    UNKNOWN_EVENT= 0
    START_EVENT_V3= 1
    QUERY_EVENT= 2
    STOP_EVENT= 3
    ROTATE_EVENT= 4
    INTVAR_EVENT= 5
    LOAD_EVENT= 6
    SLAVE_EVENT= 7
    CREATE_FILE_EVENT= 8
    APPEND_BLOCK_EVENT= 9
    EXEC_LOAD_EVENT= 10
    DELETE_FILE_EVENT= 11
    NEW_LOAD_EVENT= 12
    RAND_EVENT= 13
    USER_VAR_EVENT= 14
    FORMAT_DESCRIPTION_EVENT= 15
    XID_EVENT= 16
    BEGIN_LOAD_QUERY_EVENT= 17
    EXECUTE_LOAD_QUERY_EVENT= 18
    TABLE_MAP_EVENT = 19
    PRE_GA_WRITE_ROWS_EVENT = 20
    PRE_GA_UPDATE_ROWS_EVENT = 21
    PRE_GA_DELETE_ROWS_EVENT = 22
    WRITE_ROWS_EVENT = 23
    UPDATE_ROWS_EVENT = 24
    DELETE_ROWS_EVENT = 25
    INCIDENT_EVENT= 26
    HEARTBEAT_LOG_EVENT= 27
    IGNORABLE_LOG_EVENT= 28
    ROWS_QUERY_LOG_EVENT= 29
    WRITE_ROWS_EVENT = 30
    UPDATE_ROWS_EVENT = 31
    DELETE_ROWS_EVENT = 32
    GTID_LOG_EVENT= 33
    ANONYMOUS_GTID_LOG_EVENT= 34
    PREVIOUS_GTIDS_LOG_EVENT= 35
''''''

'''json type'''
class json_type:
    NULL_COLUMN = 251
    UNSIGNED_CHAR_COLUMN = 251
    UNSIGNED_SHORT_COLUMN = 252
    UNSIGNED_INT24_COLUMN = 253
    UNSIGNED_INT64_COLUMN = 254
    UNSIGNED_CHAR_LENGTH = 1
    UNSIGNED_SHORT_LENGTH = 2
    UNSIGNED_INT24_LENGTH = 3
    UNSIGNED_INT64_LENGTH = 8

    JSONB_TYPE_SMALL_OBJECT = 0x0
    JSONB_TYPE_LARGE_OBJECT = 0x1
    JSONB_TYPE_SMALL_ARRAY = 0x2
    JSONB_TYPE_LARGE_ARRAY = 0x3
    JSONB_TYPE_LITERAL = 0x4
    JSONB_TYPE_INT16 = 0x5
    JSONB_TYPE_UINT16 = 0x6
    JSONB_TYPE_INT32 = 0x7
    JSONB_TYPE_UINT32 = 0x8
    JSONB_TYPE_INT64 = 0x9
    JSONB_TYPE_UINT64 = 0xA
    JSONB_TYPE_DOUBLE = 0xB
    JSONB_TYPE_STRING = 0xC
    JSONB_TYPE_OPAQUE = 0xF

    JSONB_LITERAL_NULL = 0x0
    JSONB_LITERAL_TRUE = 0x1
    JSONB_LITERAL_FALSE = 0x2
''''''

BINLOG_FILE_HEADER = b'\xFE\x62\x69\x6E'
binlog_event_header_len = 19
binlog_event_fix_part = 13
binlog_quer_event_stern = 4
binlog_row_event_extra_headers = 2
read_format_desc_event_length = 56
binlog_xid_event_length = 8
table_map_event_fix_length = 8
fix_length = 8
src_length = 4

'''存储表结构数据'''
class TableMetadata:
    table_column_dict = {}
    table_pk_dict = {}