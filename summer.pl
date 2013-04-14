#!/usr/bin/perl
use strict;
my $i;
my $total;
for ($i = 1; $i <= 1000; $i++) {
	open F, "res$i" or die $!;
	<F> =~ /([\d.]+)/;
	$total += $1;
	close F;
}
my $result = $total / $i;
print "$result\n";
