# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import datetime
import decimal
import struct
import sys
sys.path.append("..")
from Binlog import  Metadata


class Read(object):
    def __init__(self,pack=None,filename=None,startposition=None):
        self.a = 123
        self.packet = pack
        self.file_data = open(filename, 'rb') if filename else None
        self.startposition = startposition if filename and startposition else None

    def read_int_be_by_size(self, size, bytes=None):
        '''Read a big endian integer values based on byte number'''
        if bytes is None:
            if size == 1:
                return struct.unpack('>b', self.read_bytes(size))[0]
            elif size == 2:
                return struct.unpack('>h', self.read_bytes(size))[0]
            elif size == 3:
                return self.read_int24_be()
            elif size == 4:
                return struct.unpack('>i', self.read_bytes(size))[0]
            elif size == 5:
                return self.read_int40_be()
            elif size == 8:
                return struct.unpack('>l', self.read_bytes(size))[0]
        else:
            '''used for read new decimal'''
            if size == 1:
                return struct.unpack('>b', bytes[0:size])[0]
            elif size == 2:
                return struct.unpack('>h', bytes[0:size])[0]
            elif size == 3:
                return self.read_int24_be(bytes)
            elif size == 4:
                return struct.unpack('>i', bytes[0:size])[0]

    def read_int24_be(self, bytes=None):
        if bytes is None:
            a, b, c = struct.unpack('BBB', self.read_bytes(3))
        else:
            a, b, c = struct.unpack('BBB', bytes[0:3])
        res = (a << 16) | (b << 8) | c
        if res >= 0x800000:
            res -= 0x1000000
        return res

    def read_uint_by_size(self, size):
        '''Read a little endian integer values based on byte number'''
        if size == 1:
            return self.read_uint8()
        elif size == 2:
            return self.read_uint16()
        elif size == 3:
            return self.read_uint24()
        elif size == 4:
            return self.read_uint32()
        elif size == 5:
            return self.read_uint40()
        elif size == 6:
            return self.read_uint48()
        elif size == 7:
            return self.read_uint56()
        elif size == 8:
            return self.read_uint64()

    def read_uint24(self):
        a, b, c = struct.unpack("<BBB", self.read_bytes(3))
        return a + (b << 8) + (c << 16)

    def read_int24(self):
        a, b, c = struct.unpack("<bbb", self.read_bytes(3))
        return a + (b << 8) + (c << 16)

    def read_uint40(self):
        a, b = struct.unpack("<BI", self.read_bytes(5))
        return a + (b << 8)

    def read_int40_be(self):
        a, b = struct.unpack(">IB", self.read_bytes(5))
        return b + (a << 8)

    def read_uint48(self):
        a, b, c = struct.unpack("<HHH", self.read_bytes(6))
        return a + (b << 16) + (c << 32)

    def read_uint56(self):
        a, b, c = struct.unpack("<BHI", self.read_bytes(7))
        return a + (b << 8) + (c << 24)

    def read_bytes(self, count):
        try:
            return self.file_data.read(count) if self.packet is None else self.packet.read(count)
        except:
            return None

    def read_uint64(self):
        read_byte = self.read_bytes(8)
        result, = struct.unpack('Q', read_byte)
        return result

    def read_int64(self):
        read_byte = self.read_bytes(8)
        result, = struct.unpack('q', read_byte)
        return result

    def read_uint32(self):
        read_byte = self.read_bytes(4)
        result, = struct.unpack('I', read_byte)
        return result

    def read_int32(self):
        read_byte = self.read_bytes(4)
        result, = struct.unpack('i', read_byte)
        return result

    def read_uint16(self):
        read_byte = self.read_bytes(2)
        result, = struct.unpack('H', read_byte)
        return result

    def read_int16(self):
        read_byte = self.read_bytes(2)
        result, = struct.unpack('h', read_byte)
        return result

    def read_uint8(self):
        read_byte = self.read_bytes(1)
        result, = struct.unpack('B', read_byte)
        return result

    def read_int8(self):
        read_byte = self.read_bytes(1)
        result, = struct.unpack('b', read_byte)
        return result

    def read_format_desc_event(self):
        binlog_ver, = struct.unpack('H', self.read_bytes(2))
        server_ver, = struct.unpack('50s', self.read_bytes(50))
        create_time, = struct.unpack('I', self.read_bytes(4))
        return binlog_ver, server_ver, create_time

    def __add_fsp_to_time(self, time, column):
        """Read and add the fractional part of time
        For more details about new date format:
        """
        microsecond, read = self.__read_fsp(column)
        if microsecond > 0:
            time = time.replace(microsecond=microsecond)
        return time, read

    def __read_fsp(self, column):
        read = 0
        if column == 1 or column == 2:
            read = 1
        elif column == 3 or column == 4:
            read = 2
        elif column == 5 or column == 6:
            read = 3
        if read > 0:
            microsecond = self.read_int_be_by_size(read)
            if column % 2:
                return int(microsecond / 10), read
            else:
                return microsecond, read

        return 0, 0

    def __read_binary_slice(self, binary, start, size, data_length):
        """
        Read a part of binary data and extract a number
        binary: the data
        start: From which bit (1 to X)
        size: How many bits should be read
        data_length: data size
        """
        binary = binary >> data_length - (start + size)
        mask = ((1 << size) - 1)
        return binary & mask

    def __read_datetime2(self, column):
        """DATETIME

        1 bit  sign           (1= non-negative, 0= negative)
        17 bits year*13+month  (year 0-9999, month 0-12)
         5 bits day            (0-31)
         5 bits hour           (0-23)
         6 bits minute         (0-59)
         6 bits second         (0-59)
        ---------------------------
        40 bits = 5 bytes
        """
        data = self.read_int_be_by_size(5)
        year_month = self.__read_binary_slice(data, 1, 17, 40)
        try:
            t = datetime.datetime(
                year=int(year_month / 13),
                month=year_month % 13,
                day=self.__read_binary_slice(data, 18, 5, 40),
                hour=self.__read_binary_slice(data, 23, 5, 40),
                minute=self.__read_binary_slice(data, 28, 6, 40),
                second=self.__read_binary_slice(data, 34, 6, 40))
        except ValueError:
            return None
        __time, read = self.__add_fsp_to_time(t, column)
        return __time, read

    def __read_time2(self, column):
        """TIME encoding for nonfractional part:

         1 bit sign    (1= non-negative, 0= negative)
         1 bit unused  (reserved for future extensions)
        10 bits hour   (0-838)
         6 bits minute (0-59)
         6 bits second (0-59)
        ---------------------
        24 bits = 3 bytes
        """
        data = self.read_int_be_by_size(3)

        sign = 1 if self.__read_binary_slice(data, 0, 1, 24) else -1
        if sign == -1:
            '''
            negative integers are stored as 2's compliment
            hence take 2's compliment again to get the right value.
            '''
            data = ~data + 1

        microseconds, read = self.__read_fsp(column)
        t = datetime.timedelta(
            hours=sign * self.__read_binary_slice(data, 2, 10, 24),
            minutes=self.__read_binary_slice(data, 12, 6, 24),
            seconds=self.__read_binary_slice(data, 18, 6, 24),
            microseconds=microseconds
        )
        return t, read + 3

    def __read_date(self):
        time = self.read_uint24()
        if time == 0:  # nasty mysql 0000-00-00 dates
            return None

        year = (time & ((1 << 15) - 1) << 9) >> 9
        month = (time & ((1 << 4) - 1) << 5) >> 5
        day = (time & ((1 << 5) - 1))
        if year == 0 or month == 0 or day == 0:
            return None

        date = datetime.date(
            year=year,
            month=month,
            day=day
        )
        return date

    def __read_new_decimal(self, precision, decimals):
        """Read MySQL's new decimal format introduced in MySQL 5"""
        '''
        Each multiple of nine digits requires four bytes, and the “leftover” digits require some fraction of four bytes. 
        The storage required for excess digits is given by the following table. Leftover Digits	Number of Bytes

        Leftover Digits     Number of Bytes
        0	                0
        1	                1
        2	                1
        3	                2
        4	                2
        5	                3
        6	                3
        7	                4
        8	                4

        '''
        digits_per_integer = 9
        compressed_bytes = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4]
        integral = (precision - decimals)
        uncomp_integral = int(integral / digits_per_integer)
        uncomp_fractional = int(decimals / digits_per_integer)
        comp_integral = integral - (uncomp_integral * digits_per_integer)
        comp_fractional = decimals - (uncomp_fractional
                                      * digits_per_integer)

        _read_bytes = (uncomp_integral * 4) + (uncomp_fractional * 4) + compressed_bytes[comp_fractional] + \
                      compressed_bytes[comp_integral]

        _data = bytearray(self.read_bytes(_read_bytes))
        value = _data[0]
        if value & 0x80 != 0:
            res = ""
            mask = 0
        else:
            mask = -1
            res = "-"
        _data[0] = struct.pack('<B', value ^ 0x80)

        size = compressed_bytes[comp_integral]
        offset = 0
        if size > 0:
            offset += size
            value = self.read_int_be_by_size(size=size, bytes=_data) ^ mask
            res += str(value)

        for i in range(0, uncomp_integral):
            offset += 4
            value = struct.unpack('>i', _data[offset - 4:offset])[0] ^ mask
            res += '%09d' % value

        res += "."

        for i in range(0, uncomp_fractional):
            offset += 4
            value = struct.unpack('>i', _data[offset - 4:offset])[0] ^ mask
            res += '%09d' % value

        size = compressed_bytes[comp_fractional]
        if size > 0:
            value = self.read_int_be_by_size(size=size, bytes=_data[offset:]) ^ mask
            res += '%0*d' % (comp_fractional, value)

        return decimal.Decimal(res), _read_bytes

    def is_null(self, null_bitmap, position):
        bit = null_bitmap[int(position / 8)]
        if type(bit) is str:
            bit = ord(bit)
        return bit & (1 << (position % 8))

    '''parsing for json'''
    '''################################################################'''

    def read_binary_json(self, length):
        t = self.read_uint8()
        return self.read_binary_json_type(t, length)

    def read_binary_json_type(self, t, length):
        large = (t in (Metadata.json_type.JSONB_TYPE_LARGE_OBJECT, Metadata.json_type.JSONB_TYPE_LARGE_ARRAY))
        if t in (Metadata.json_type.JSONB_TYPE_SMALL_OBJECT, Metadata.json_type.JSONB_TYPE_LARGE_OBJECT):
            return self.read_binary_json_object(length - 1, large)
        elif t in (Metadata.json_type.JSONB_TYPE_SMALL_ARRAY, Metadata.json_type.JSONB_TYPE_LARGE_ARRAY):
            return self.read_binary_json_array(length - 1, large)
        elif t in (Metadata.json_type.JSONB_TYPE_STRING,):
            return self.read_length_coded_pascal_string(1)
        elif t in (Metadata.json_type.JSONB_TYPE_LITERAL,):
            value = self.read_uint8()
            if value == Metadata.json_type.JSONB_LITERAL_NULL:
                return None
            elif value == Metadata.json_type.JSONB_LITERAL_TRUE:
                return True
            elif value == Metadata.json_type.JSONB_LITERAL_FALSE:
                return False
        elif t == Metadata.json_type.JSONB_TYPE_INT16:
            return self.read_int16()
        elif t == Metadata.json_type.JSONB_TYPE_UINT16:
            return self.read_uint16()
        elif t in (Metadata.json_type.JSONB_TYPE_DOUBLE,):
            return struct.unpack('<d', self.read(8))[0]
        elif t == Metadata.json_type.JSONB_TYPE_INT32:
            return self.read_int32()
        elif t == Metadata.json_type.JSONB_TYPE_UINT32:
            return self.read_uint32()
        elif t == Metadata.json_type.JSONB_TYPE_INT64:
            return self.read_int64()
        elif t == Metadata.json_type.JSONB_TYPE_UINT64:
            return self.read_uint64()

        raise ValueError('Json type %d is not handled' % t)

    def read_binary_json_type_inlined(self, t):
        if t == Metadata.json_type.JSONB_TYPE_LITERAL:
            value = self.read_uint16()
            if value == Metadata.json_type.JSONB_LITERAL_NULL:
                return None
            elif value == Metadata.json_type.JSONB_LITERAL_TRUE:
                return True
            elif value == Metadata.json_type.JSONB_LITERAL_FALSE:
                return False
        elif t == Metadata.json_type.JSONB_TYPE_INT16:
            return self.read_int16()
        elif t == Metadata.json_type.JSONB_TYPE_UINT16:
            return self.read_uint16()
        elif t == Metadata.json_type.JSONB_TYPE_INT32:
            return self.read_int32()
        elif t == Metadata.json_type.JSONB_TYPE_UINT32:
            return self.read_uint32()

        raise ValueError('Json type %d is not handled' % t)

    def read_binary_json_object(self, length, large):
        if large:
            elements = self.read_uint32()
            size = self.read_uint32()
        else:
            elements = self.read_uint16()
            size = self.read_uint16()

        if size > length:
            raise ValueError('Json length is larger than packet length')

        if large:
            key_offset_lengths = [(
                self.read_uint32(),  # offset (we don't actually need that)
                self.read_uint16()  # size of the key
            ) for _ in range(elements)]
        else:
            key_offset_lengths = [(
                self.read_uint16(),  # offset (we don't actually need that)
                self.read_uint16()  # size of key
            ) for _ in range(elements)]

        value_type_inlined_lengths = [self.read_offset_or_inline(large)
                                      for _ in range(elements)]

        keys = [self.__read_decode(x[1]) for x in key_offset_lengths]

        out = {}
        for i in range(elements):
            if value_type_inlined_lengths[i][1] is None:
                data = value_type_inlined_lengths[i][2]
            else:
                t = value_type_inlined_lengths[i][0]
                data = self.read_binary_json_type(t, length)
            out[keys[i]] = data

        return out

    def read_binary_json_array(self, length, large):
        if large:
            elements = self.read_uint32()
            size = self.read_uint32()
        else:
            elements = self.read_uint16()
            size = self.read_uint16()

        if size > length:
            raise ValueError('Json length is larger than packet length')

        values_type_offset_inline = [self.read_offset_or_inline(large) for _ in range(elements)]

        def _read(x):
            if x[1] is None:
                return x[2]
            return self.read_binary_json_type(x[0], length)

        return [_read(x) for x in values_type_offset_inline]

    def read_offset_or_inline(self, large):
        t = self.read_uint8()

        if t in (Metadata.json_type.JSONB_TYPE_LITERAL,
                 Metadata.json_type.JSONB_TYPE_INT16, Metadata.json_type.JSONB_TYPE_UINT16):
            return (t, None, self.read_binary_json_type_inlined(t))
        if large and t in (Metadata.json_type.JSONB_TYPE_INT32, Metadata.json_type.JSONB_TYPE_UINT32):
            return (t, None, self.read_binary_json_type_inlined(t))

        if large:
            return (t, self.read_uint32(), None)
        return (t, self.read_uint16(), None)

    def read_length_coded_pascal_string(self, size):
        """Read a string with length coded using pascal style.
        The string start by the size of the string
        """
        length = self.read_uint_by_size(size)
        return self.__read_decode(length)

    '''###################################################'''