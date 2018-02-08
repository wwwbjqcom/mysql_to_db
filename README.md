# mysql_to_db
该工具可以结合mysqldump备份工具实现在线迁移库、表，主要功能通过提供的binlog文件及position模拟slave追加数据到目标库
功能：
	1、通过binlog追加dml数据到目标库
	2、可通过库名、表名过滤
	3、实时记录处理到的binlog位置，可断点续传

可通过-h、--help查看参数帮助：
	Usage:
    	Options:
      		-h [--help] : print help message
      		-f [--binlogfile] : the  binlog file name
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
			
注意项：
	1、实时记录的binlog是通过临时库表实现，所以账号权限需有对应库的权限，库名为dump2db
	2、需要information_schema.columns表的查询权限以及replication slave权限
	3、目标库增删改权限
	4、只实现了dml语句产生的数据追加操作