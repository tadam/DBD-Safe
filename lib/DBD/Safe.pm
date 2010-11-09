package DBD::Safe;

use strict;
use warnings;

#ABSTRACT: keep safe connection to DB

=head1 SYNOPSIS

  use DBI;
  my $dbh = DBI->connect(
      'DBI:Safe:', undef, undef,
      { dbi_connect_args => [$dsn, $user, $password, $args] }
  );

=head1 DESCRIPTION

DBD::Safe is an abstract DBI driver that helps you to keep safe connection to
your database. Its purpose is reconnection to database when connection was corrupted.
DBD::Safe makes reconnection in the following cases:
  - connection was dropped (usually occurs in long-running processes)
  - process was forked or threaded
DBD::Safe throws exception if reconnection needed during the transaction.

=head1 WHY YET ANOTHER SOLUTION?

CPAN contains modules with similar functionality. On the first place it is a
L<DBIx::Connector>, also see L<DBIx::HA> and L<DBIx::DWIW>.
But DBIx::Connector and DBIx::DWIW assumes own interface for interacting with
database. If you are going to use DBIx::Connector you must explicitly call
$conn->dbh to get a real dbh connection. And if you want to add some fault tolerance
in a tons of existed code, you must refactor all this code where you use database
connections.

DBD::Safe have a transparent interface. You just need to replace C<connect()> options
and after this you can use it as usual database handler.

=head1 METHODS

=over

=item C<connect>

For using DBD::Safe use DBI in a such manner:

  my $dbh = DBI->connect('DBI:Safe:', undef, undef, $dbd_safe_args);

All arguments for DBD::Safe passes in the C<$dbd_safe_args> hashref.
This hashref can have following keys:

=over

=item I<dbi_connect_args>

It is an arrayref with arguments for DBI->connect() which you passes when you
use DBI without DBD::Safe. These arguments will be used for (re)connection to
your database

=item I<connect_cb>

Instead of passing C<dbi_connect_args> you can pass coderef that will be called
during (re)connection. This coderef must return database handler. Using
C<connect_cb> you can switch to another replica in case of disconnection or
implement another logic.

You must pass any of C<dbi_connect_args> or C<connect_cb>.

=item I<retry_cb>

This callback uses every time when DBD::Safe decides that reconnection needed.
By default DBD::Safe make only one trie to reconnect and dies if it was
unsuccessful. You can override this using C<retry_cb>.
This callback takes one argument - number of reconnection trie and returns
true or false (to make another reconnection attempt or not). You can place
some C<sleep()> in this callback depending on number of trie.

=item I<reconnect_period>

If you want automatically reconnect after some time you can use this key.
Reconnect will occur after C<reconnect_period> seconds.

=back

=item C<x_safe_get_dbh>

If you have DBI with version >= 1.54, then you can explicitly call

  my $real_dbh = $safe_dbh->x_safe_get_dbh;

This method will return real database handler that uses in the current time.

If you have DBI with version < 1.54, you can call

  my $real_dbh = $safe_dbh->func('x_safe_get_dbh');

=back

=head1 SEE ALSO

L<http://github.com/tadam/DBD-Safe>,
L<DBIx::Connector>, L<DBIx::HA>, L<DBIx::DWIW>.

=cut

use base qw(DBD::File);

use vars qw($err $errstr $sqlstate $drh);

sub DESTROY {
    shift->STORE(Active => 0);
}

$err      = 0;  # DBI::err
$errstr   = ""; # DBI::errstr
$sqlstate = ""; # DBI::state
$drh      = undef;

sub driver {
    my ($class, $attr) = @_;
    return $drh if $drh;

    DBI->setup_driver($class);

    # x_<smth> allowed only from 1.54
    if ($DBI::VERSION > 1.53) {
        DBD::Safe::db->install_method('x_safe_get_dbh');
    }

    my $self = $class->SUPER::driver({
        Name        => 'Safe',
        Version     => $DBD::Safe::VERSION,
        Err         => \$DBD::Safe::err,
        Errstr      => \$DBD::Safe::errstr,
        State       => \$DBD::Safe::sqlstate,
        Attribution => 'DBD::Safe',
    });
    return $self;
}

#######################################################################
package DBD::Safe::dr;

use strict;
use warnings;

$DBD::Safe::dr::imp_data_size = 0;
use DBD::File;
use DBI qw();
use base qw(DBD::File::dr);

sub connect {
    my($drh, $dbname, $user, $auth, $attr) = @_;
    my $connect_cb;
    if ($attr->{connect_cb}) {
        $connect_cb = $attr->{connect_cb};
    } elsif ($attr->{dbi_connect_args}) {
        $connect_cb = sub { DBI->connect(@{$attr->{dbi_connect_args}}) };
    } else {
        die "No connect way defined\n";
        #return $drh->set_err($DBI::stderr, "No connect way defined");
    }

    my $retry_cb = sub {
        my $trie = shift;
        if ($trie == 1) {
            return 1;
        } else {
            return 0;
        }
    };
    $retry_cb = $attr->{retry_cb} if ($attr->{retry_cb});


    my $reconnect_period = $attr->{reconnect_period};

    my $dbh = DBI::_new_dbh(
      $drh => {
               Name         => 'safedb',
               USER         => $user,
               CURRENT_USER => $user,
              },
    );
    $dbh->STORE(Active => 1);

    $dbh->STORE('x_safe_connect_cb'       => $connect_cb);
    $dbh->STORE('x_safe_state'            => {});
    $dbh->STORE('x_safe_reconnect_period' => $reconnect_period);
    $dbh->STORE('x_safe_retry_cb'         => $retry_cb);

    return $dbh;
}

#######################################################################
package DBD::Safe::db;

use strict;
use warnings;

$DBD::Safe::db::imp_data_size = 0;

use vars qw($AUTOLOAD);

sub prepare;
sub column_info;

sub begin_work {
    my $dbh = shift;
    my $in_transaction = $dbh->FETCH('x_safe_in_transaction');
    $in_transaction++;
    $dbh->STORE('x_safe_in_transaction', $in_transaction);
    return _proxy_method('begin_work', $dbh, @_);
}

sub commit {
    my $dbh = shift;
    my $in_transaction = $dbh->FETCH('x_safe_in_transaction');
    $in_transaction--;
    if ($in_transaction < 0) {
        $in_transaction = 0;
        warn "commit() without begin_work()\n";
        #$dbh->set_err(0, "commit() without begin_work()");
    }
    $dbh->STORE('x_safe_in_transaction', $in_transaction);
    return _proxy_method('commit', $dbh, @_);
}

sub rollback {
    my $dbh = shift;
    my $in_transaction = $dbh->FETCH('x_safe_in_transaction');
    $in_transaction--;
    if ($in_transaction < 0) {
        $in_transaction = 0;
        warn "rollback() without begin_work()\n";
        #$dbh->set_err(0, "rollback() without begin_work()");
    }
    $dbh->STORE('x_safe_in_transaction', $in_transaction);
    return _proxy_method('rollback', $dbh, @_);
}

sub _proxy_method {
    my ($method, $dbh, @args) = @_;
    my $state = $dbh->FETCH('x_safe_state');
    my $real_dbh = stay_connected($dbh);
    return $real_dbh->$method(@args);
}

# TODO: take a more accurate logic from DBD::Proxy
sub AUTOLOAD {
    my $method = $AUTOLOAD;
    $method =~ s/(.*::(.*)):://;
    my $class = $1;
    my $type = $2;

    my $s = sub { return _proxy_method($method, @_) };

    no strict 'refs';
    *{$AUTOLOAD} = $s;
    goto &$s;
}

sub x_safe_get_dbh {
    my $dbh = shift;
    my $real_dbh = stay_connected($dbh);
    return $real_dbh;
}


sub disconnect {
    my ($dbh) = @_;

    $dbh->STORE(Active => 0);

    1;
}

sub STORE {
    my ($dbh, $attr, $val) = @_;

    if ($attr =~ /^(x_safe_|Active$)/) {
        $dbh->{$attr} = $val;

        # because of some old DBI bug
        if ($attr eq 'Active') {
            my $v = $real_dbh->FETCH($attr);
        }
    } else {
        my $real_dbh = stay_connected($dbh);
        $real_dbh->STORE($attr => $val);
    }
}

sub FETCH {
    my ($dbh, $attr) = @_;

    if ($attr =~ /^(x_safe_|Active$)/) {
        return $dbh->{$attr};
    } else {
        my $real_dbh = stay_connected($dbh);
        return $real_dbh->FETCH($attr);
    }
}

sub DESTROY {
    my $dbh = shift;
    $dbh->disconnect;
}

sub stay_connected {
    my $dbh = shift;

    my $state = $dbh->FETCH('x_safe_state');
    my $in_transaction = $dbh->FETCH('x_safe_in_transaction');
    my $last_connected = $dbh->FETCH('x_safe_last_connected');
    my $reconnect_period = $dbh->FETCH('x_safe_reconnect_period');

    my $reconnect = 0;
    if ($state->{dbh}) {
        if (
            (defined($state->{tid}) && $state->{tid} != threads->tid) ||
            ($state->{pid} != $$) ||
            (!is_connected($dbh)) ||
            (
             $reconnect_period && $last_connected &&
             (time() - $last_connected > $reconnect_period)
            )
           )
        {
            $reconnect = 1;

            if ($state->{pid} != $$) {
                $state->{dbh}->{InactiveDestroy} = 1;
            }
        }
    } else {
        $reconnect = 1;
    }

    if ($reconnect) {
        if ($in_transaction) {# || ($state->{dbh} && !$state->{dbh}->{AutoCommit})) {
            die "Reconnect needed when db in transaction\n";
            #return $dbh->set_err($DBI::stderr, "Reconnect needed when db in transaction");
        }

        my $trie = 0;
        my $retry_cb = $dbh->FETCH('x_safe_retry_cb');
        while (1) {
            $trie++;
            my $can_connect = $retry_cb->($trie);
            if ($can_connect) {
                my $dbh = eval { real_connect($dbh) };
                if (!$dbh) {
                    next;
                } else {
                    $state->{dbh} = $dbh;
                    last;
                }
            } else {
                my $error = $state->{last_error} || '';
                chomp($error);
                die "All tries to connect is ended, can't connect: [$error]\n";
                #return $dbh->set_err(
                #    $DBI::stderr,
                #    "All tries to connect is ended, can't connect: [$error]"
                #);
            }
        }
    }

    return $state->{dbh};
}

sub is_connected {
    my $dbh = shift;

    my $state = $dbh->FETCH('x_safe_state');

    return $state->{dbh}->{Active} && $state->{dbh}->ping;
}

sub real_connect {
    my $dbh = shift;

    my $connect_cb = $dbh->FETCH('x_safe_connect_cb');
    my $state = $dbh->FETCH('x_safe_state');

    my $real_dbh;
    eval {
        $real_dbh = $connect_cb->();
    };
    if ($@) {
        $state->{last_error} = $@;
    } else {
        $dbh->STORE('x_safe_last_connected', time());
    }
    $state->{pid} = $$;
    $state->{tid} = threads->tid if $INC{'threads.pm'};

    return $real_dbh;
}

1;
