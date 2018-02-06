# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import os,time
from System import UsePlatform

class SaveSql:
    def __init__(self):
        self.path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        self.f = None

    def __CheckSystem(self):
        sys = UsePlatform()
        if sys == 'Linux':
            return self.path
        elif sys == 'Windows':
            return self.path.replace('\\','/')

    def __create_file(self):
        date = time.strftime('%Y%m%d',time.localtime(time.time()))
        file_path = '{}/var/{}.sql'.format(self.__CheckSystem(),date)
        self.f = file(file_path,'wb')

    def ToFile(self,sql_list):
        self.__create_file()
        self.__create_file()
        if len(sql_list) > 0:
            for sql in sql_list:
                self.f.write('{}\n'.format(sql))

        self.f.close()