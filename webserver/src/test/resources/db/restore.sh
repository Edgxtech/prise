#!/bin/sh
/usr/bin/gunzip < src/test/resources/db/$3 | /usr/local/mysql/bin/mysql -u $1 -p$2 $4
