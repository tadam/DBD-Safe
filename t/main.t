#!/usr/bin/perl

package Test::DBD::Safe;

use strict;
use warnings;

use DBI;

use Test::Class;
use Test::Exception;
use Test::More;

use base qw(Test::Class);

sub connect : Test(2) {
    use_ok('DBD::Safe');
    my $dbh = get_dbh();
    ok($dbh);
}

sub reconnect : Test(2) {
    my $dbh = get_dbh();

    my $parent_real_dbh = $dbh->func('x_safe_get_dbh');

    my $pid = open(CHILD_WRITE, "-|");

    if ($pid) {
        my $child_real_dbh;
        while (my $l = <CHILD_WRITE>) {
            $child_real_dbh .= $l;
        }
        chomp($child_real_dbh);
        close(CHILD_WRITE);
        isnt("$child_real_dbh", "$parent_real_dbh", "reconnect in child after fork()");
    } else {
        my $child_real_dbh = $dbh->func('x_safe_get_dbh');
        print "$child_real_dbh\n";
        exit();
    }

    my $parent_real_dbh2 = $dbh->func('x_safe_get_dbh');
    is("$parent_real_dbh", "$parent_real_dbh2", "parent dbh not changed since fork()");
}

sub get_dbh {
    my $dbh = DBI->connect('DBI:Safe:', undef, undef,
        {
         dbi_connect_args => ['dbi:ExampleP:dummy', '', '']
        }
    );
    return $dbh;
}

# валидация параметров
# реконнект после:
# - разрыва соединения
# retry_cb
# reconnect_period
# PrintError/RaiseError/etc
# x_safe_get_dbh

Test::Class->runtests(
    __PACKAGE__->new
);

1;

