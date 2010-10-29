#!/usr/bin/perl

package Test::DBD::Safe;

use strict;
use warnings;

use DBI;

use Test::Class;
use Test::Exception;
use Test::More;

use base qw(Test::Class);

sub _begin : Test {
    use_ok('DBD::Safe');
}

sub _connect : Test {
    my $dbh = DBI->connect('DBI:Safe:', undef, undef,
        {
         dbi_connect_args => ['dbi:ExampleP:dummy', '', '']
        }
    );
    ok($dbh);
}

# валидация параметров
# реконнект после:
# - разрыва соединения
# - форка
# retry_cb
# reconnect_period
# PrintError/RaiseError/etc
# x_safe_get_dbh

Test::Class->runtests(
    __PACKAGE__->new
);

1;

