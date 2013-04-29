#!/bin/bash

rm -rf ~/dbtests
rm -rf ~/dbdata
echo "\timing" > ~/.psqlrc
./configure --prefix=$HOME/dbtests --enable-debug --enable-cassert
make
make install
~/dbtests/bin/initdb -D ~/dbdata
~/dbtests/bin/pg_ctl -D ~/dbdata start && sleep 1s
if [ $? -eq 0 ]; then

	# page per operator tests
	~/dbtests/bin/psql template1 -c "CREATE DATABASE scantest;" && sleep 1s
	~/dbtests/bin/psql scantest -c "CREATE TABLE test_table (field INT);" && sleep 1s
	for i in `seq 1 3000`; do
		~/dbtests/bin/psql scantest -c "INSERT INTO test_table (field) VALUES($i);"
	done

	# nomvcc tests
	~/dbtests/bin/psql template1 -c "CREATE DATABASE nomvcctest;" && sleep 1s
	~/dbtests/bin/psql nomvcctest -c "CREATE TABLE test_table (field INT);" && sleep 1s
	~/dbtests/bin/psql nomvcctest -c "INSERT INTO test_table (field) VALUES(2);"

fi
~/dbtests/bin/pg_ctl -D ~/dbdata stop && sleep 1s
