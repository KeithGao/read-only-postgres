#!/usr/bin/perl
use strict;

print `~/pgsql/bin/pg_ctl -D ~/pgdata -l pre.log start`;
sleep 1;

my $counter;
my $i;
for ($i = 0; $i < 1000; $i++) {
	$_ = `echo 'SELECT * FROM test_table;' | ~/pgsql/bin/psql valdb`;
	/Time: (\d+\.\d+)/;
	$counter += $1;
}
print "Average time: ", $counter / $i, "\n";
print `~/pgsql/bin/pg_ctl -D ~/pgdata stop`;

# original time: 0.768125000000001
# modified time: 0.766377000000001
# that's just a 0.227567% increase. Not very good