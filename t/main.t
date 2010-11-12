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

sub _connect : Test(3) {
    use_ok('DBD::Safe');
    my $dbh = get_dbh();
    ok($dbh);
    my $rdbh1 = $dbh->func('x_safe_get_dbh');
    my $rdbh2 = $dbh->func('x_safe_get_dbh');
    is("$rdbh2", "$rdbh1", "don't reconnect in good cases");
}

sub _x_safe_get_dbh : Test(4) {
    my $dbh = get_dbh();
    my $rdbh = $dbh->func('x_safe_get_dbh');

    my $test = sub {
        isnt("$rdbh", "$dbh", "dbh and real_dbh is different");
        is($rdbh->{Driver}->{Name}, "ExampleP", "real_dbh is really real");
    };
    $test->();

    if ($DBI::VERSION <= 1.53) {
        return ("\$DBI::VERSION <= 1.53, don't test implicit call of x_safe_get_dbh");
    }

    $rdbh = $dbh->x_safe_get_dbh;
    $test->();
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

sub retry_cb : Test(1) {
    my $cb = sub {
        my $trie = shift;
        return 0
    };
    dies_ok(sub { get_dbh({retry_cb => $cb}) }, "always negative retry_cb");
}

sub reconnect_cb : Test(2) {
    my $last_connected;
    my $cb = sub {
        use Time::HiRes qw(time);
        my $dbh = shift;
        my $t = time();
        if (!defined($last_connected) ||
            ($t - $last_connected >= 1))
        {
            $last_connected = $t;
            return 1;
        } else {
            return 0;
        }
    };
    my $dbh = get_dbh({reconnect_cb => $cb});
    my $rdbh1 = $dbh->func('x_safe_get_dbh');
    my $rdbh2 = $dbh->func('x_safe_get_dbh');
    sleep(1);
    my $rdbh3 = $dbh->func('x_safe_get_dbh');

    is("$rdbh2", "$rdbh1", "don't use reconnect_cb when it is not needed");
    isnt("$rdbh3", "$rdbh2", "reconnected using reconnect_cb");
}

sub get_dbh {
    my $attr = shift || {};
    my $dbh = DBI->connect('DBI:Safe:', undef, undef,
        {
         dbi_connect_args => ['dbi:ExampleP:dummy', '', ''],
         %{$attr},
        }
    );
    return $dbh;
}

# валидация параметров
# реконнект после разрыва соединения
# PrintError/RaiseError/etc

Test::Class->runtests(
    __PACKAGE__->new
);

1;

