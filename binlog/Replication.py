# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import struct,pymysql


class ReplicationMysql:
    def __init__(self, server_id=None, log_file=None,
                 log_pos=None,mysql_connection=None):

        self._log_file = log_file
        self._log_pos = log_pos
        self.block = True
        self.server_id = server_id if server_id != None else 133
        self.connection = mysql_connection

    def __checksum_enabled(self):
        """Return True if binlog-checksum = CRC32. Only for MySQL > 5.6"""
        with self.connection.cursor() as cur:
            cur.execute('SET SESSION wait_timeout = 2147483;')
            sql = 'SHOW GLOBAL VARIABLES LIKE "BINLOG_CHECKSUM";'
            cur.execute(sql)
            result = cur.fetchone()

        if result is None:
            return False
        if 'Value' in result and result['Value'] is None:
            return False
        return True

    def __set_checksum(self):
        with self.connection.cursor() as cur:
            cur.execute("set @master_binlog_checksum= @@global.binlog_checksum;")

    def GetFile(self):
        with self.connection.cursor() as cur:
            sql = "show master status;"
            cur.execute(sql)
            result = cur.fetchone()
            return result['File'], result['Position']

    def PackeByte(self):
        '''
        Format for mysql packet position
        file_length: 4bytes
        dump_type: 1bytes
        position: 4bytes
        flags: 2bytes  
            0: BINLOG_DUMP_BLOCK
            1: BINLOG_DUMP_NON_BLOCK
        server_id: 4bytes
        log_file
        :return: 
        '''
        COM_BINLOG_DUMP = 0x12

        if self._log_file is None:
            if self._log_pos is None:
                self._log_file, self._log_pos = self.GetFile()
            else:
                self._log_file, _ = self.GetFile()
        elif self._log_file and self._log_pos is None:
            self._log_pos = 4

        prelude = struct.pack('<i', len(self._log_file) + 11) \
                  + struct.pack("!B", COM_BINLOG_DUMP)

        prelude += struct.pack('<I', self._log_pos)
        if self.block:
            prelude += struct.pack('<h', 0)
        else:
            prelude += struct.pack('<h', 1)

        prelude += struct.pack('<I', self.server_id)
        prelude += self._log_file.encode()
        return prelude

    def ReadPack(self):
        _packet = self.PackeByte()
        if self.__checksum_enabled():
            self.__set_checksum()

        if pymysql.__version__ < "0.6":
            self.connection.wfile.write(_packet)
            self.connection.wfile.flush()
        else:
            self.connection._write_bytes(_packet)
            self.connection._next_seq_id = 1

        return self.connection

        '''
        while True:
            try:
                if pymysql.__version__ < "0.6":
                    pkt = self.connection.read_packet()
                else:
                    pkt = self.connection._read_packet()

                self.UnPack(pkt)
            except:
                self.connection.close()
                break
        '''
