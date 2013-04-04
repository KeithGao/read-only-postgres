#!/usr/bin/perl
use strict;

# schema has been set up as one table with one field and one inserted tuple

print `~/pgsql/bin/pg_ctl -D ~/pgdata -l pre.log start`;
sleep 1;

my $counter;
my $i;
for ($i = 0; $i < 1000; $i++) {
	$_ = `echo 'SELECT * FROM test_table;' | ~/pgsql/bin/psql valdb`;
	/Time: (\d+\.\d+)/;
	#print "Time: $1\n";
	$counter += $1;
	#sleep 1;
}
print "Average time: ", $counter / $i, "\n";
print `~/pgsql/bin/pg_ctl -D ~/pgdata stop`;

# original time: 0.768125000000001
# modified time: 0.766377000000001