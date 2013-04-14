#!/bin/bash
~/dbtests/bin/pg_ctl -D ~/dbdata start && sleep 1s
if [ $? -eq 0 ]; then

	# page per operator scan test
	for i in `seq 1 1000`; do
		echo "SELECT SUM(field) FROM test_table WHERE field % 2 != 0;" | ~/dbtests/bin/psql scantest | grep Time > res$i
	done
	echo "Average time for sequential scan: "
	./summer.pl
	rm res*
	
fi
~/dbtests/bin/pg_ctl -D ~/dbdata stop && sleep 1s
