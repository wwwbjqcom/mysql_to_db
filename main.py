# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import sys,getopt
from lib import entrance


def Usage():
    __usage__ = """
    	Usage:
    	Options:
      		-h [--help] : print help message
      		-f [--binlogfile] : the file path
      		--start-position : Start reading the binlog at position N. Applies to the
                                    first binlog passed on the command line.
            -t [--tables] : table name list ,"t1,t2,t3"
            -D [--databases] : database name list ,"db1,db2,db3"
            -u [--user] : User for login if not current user
            -p [--passwd] : Password to use when connecting to server
            -H [--host] : Connect to host, default localhost
            -P [--port] : Port number to use for connection ,default 3306
            -S [--socket] : mysql unix socket
            --dhost : destination mysql host
            --dport : destination mysql port
            --duser : destination mysql user name
            --dpasswd : destination mysql password
    	    """
    print __usage__


def main(argv):
    _argv = {}
    try:
        opts, args = getopt.getopt(argv[1:], 'hf:H:u:p:P:D:t:S:',
                                   ['help', 'binlogfile=', 'start-position=', 'host=', 'user=', 'passwd=',
                                    'port=', 'database=', 'tables=','dhost=','dport=','duser=','dpasswd=',
                                    'socket='])
    except getopt.GetoptError, err:
        print str(err)
        Usage()
        sys.exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            Usage()
            sys.exit(1)
        elif o in ('-f', '--binlogfile'):
            _argv['file'] = a
        elif o in ('--start-position',):
            _argv['start-position'] = int(a)
        elif o in ('-u', '--user'):
            _argv['user'] = a
        elif o in ('-H', '--host'):
            _argv['host'] = a
        elif o in ('-p', '--passwd'):
            _argv['passwd'] = a
        elif o in ('-P', '--port'):
            _argv['port'] = int(a)
        elif o in ('-t','--tables'):
            _argv['tables'] = a
        elif o in ('-D','--database'):
            _argv['database'] = a
        elif o in ('--dhost'):
            _argv['dhost'] = a
        elif o in ('--dport'):
            _argv['dport'] = int(a)
        elif o in ('--duser'):
            _argv['duser'] = a
        elif o in ('--dpasswd'):
            _argv['dpasswd'] = a
        elif o in ('-S','--socket'):
            _argv['socket'] = a
        else:
            print 'unhandled option'
            sys.exit(3)

    with entrance.Entrance(_argv):
        pass


if __name__ == "__main__":
    main(sys.argv)
