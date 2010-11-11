#!/usr/bin/perl

package Test::DBD::Safe;

use strict;
use warnings;

use lib qw(lib);

use DBI;

use Test::Class;
use Test::Exception;
use Test::More;

use base qw(Test::Class);

sub connect : Test(3) {
    use_ok('DBD::Safe');
    my $dbh = get_dbh();
    ok($dbh);
    my $rdbh1 = $dbh->func('x_safe_get_dbh');
    my $rdbh2 = $dbh->func('x_safe_get_dbh');
    is("$rdbh2", "$rdbh1", "don't reconnect in good cases");
}

sub reconnect_ping : Test(1) {
    my $dbh = get_dbh();
    my $rdbh1 = $dbh->func('x_safe_get_dbh');

    no strict 'refs';
    no warnings;
    local *{'DBD::ExampleP::db::ping'} = sub { 0 };
    my $rdbh2 = $dbh->func('x_safe_get_dbh');
    isnt("$rdbh2", "$rdbh1", "reconnect if ping is negative");
}

sub reconnect_fork : Test(2) {
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

sub reconnect_threads : Test(1) {
    no strict 'refs';
    local $INC{'threads.pm'} = 1;
    local *{'threads::tid'} = sub { 42 };

    my $dbh = get_dbh();
    my $real_dbh1 = $dbh->func('x_safe_get_dbh');
    my $state = $dbh->FETCH('x_safe_state');
    $state->{tid} = 43;

    my $real_dbh2 = $dbh->func('x_safe_get_dbh');
    isnt("$real_dbh2", "$real_dbh1", "reconnect if threads()");
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

