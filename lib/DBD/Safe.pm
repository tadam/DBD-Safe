package DBD::Safe;

use strict;
use warnings;

=head1 NAME

DBD::Safe - keep safe connection to DB

=head1 SYNOPSIS

=head1 DESCRIPTION

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
        Version     => $VERSION,
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
    my $connect_func;
    if ($attr->{connect_func}) {
        $connect_func = $attr->{connect_func};
    } elsif ($attr->{dbi_connect_args}) {
        $connect_func = sub { DBI->connect(@{$attr->{dbi_connect_args}}) };
    } else {
        die "No connect way defined\n";
    }

    my $reconnect_period;
    if ($attr && ref($attr) eq 'HASH') {
        $reconnect_period = $attr->{reconnect_period};
        if (!$dbname) {
            $dbname = $attr->{dbname} || '';
        }
    }

    my $dbh = DBI::_new_dbh(
      $drh => {
               Name         => $dbname,
               USER         => $user,
               CURRENT_USER => $user,
              },
    );
    $dbh->STORE(Active => 1);

    $dbh->STORE('x_safe_connect_func'     => $connect_func);
    $dbh->STORE('x_safe_state'            => {});
    $dbh->STORE('x_safe_reconnect_period' => $reconnect_period);
    $dbh->STORE('x_safe_dbname'           => $dbname);

    return $dbh;
}

sub data_sources { my @sources = (); return @sources; }


#######################################################################
package DBD::Safe::db;

use strict;
use warnings;

$DBD::Safe::db::imp_data_size = 0;

use vars qw($AUTOLOAD);

sub prepare;

sub begin_work {
    my $dbh = shift;
    $dbh->STORE('x_safe_in_transaction', 1);
    return _proxy_method('begin_work', $dbh, @_);
}

sub commit {
    my $dbh = shift;
    $dbh->STORE('x_safe_in_transaction', 0);
    return _proxy_method('commit', $dbh, @_);
}

sub rollback {
    my $dbh = shift;
    $dbh->STORE('x_safe_in_transaction', 0);
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
    } else {
        my $real_dbh = stay_connected($dbh);
        $real_dbh->STORE($attr => $val);
        if ($attr eq 'Active') {
            my $v = $real_dbh->FETCH($attr);
        }
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
    shift->disconnect;
}

sub stay_connected {
    my $dbh = shift;

    my $state = $dbh->FETCH('x_safe_state');
    my $in_transaction = $dbh->FETCH('x_safe_in_transaction');
    my $last_connected = $dbh->FETCH('x_safe_last_connected');
    my $reconnect_period = $dbh->FETCH('x_safe_reconnect_period');
    my $dbname = $dbh->FETCH('x_safe_dbname');

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
        if ($in_transaction) {
            die "Reconnect needed when db [$dbname] in transaction";
        }

        $state->{dbh} = real_connect($dbh);
    }

    unless ($state->{dbh}) {
        die $state->{last_error};
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

    my $connect_func = $dbh->FETCH('x_safe_connect_func');
    my $state = $dbh->FETCH('x_safe_state');
    my $dbname = $dbh->FETCH('x_safe_dbname');

    my $real_dbh;
    eval {
        $real_dbh = $connect_func->();
    };
    if ($@) {
        $state->{last_error} = $@;
        warn "Failed to connect to [$dbname]";
    } else {
        $dbh->STORE('x_safe_last_connected', time());
    }
    $state->{pid} = $$;
    $state->{tid} = threads->tid if $INC{'threads.pm'};

    return $real_dbh;
}

1;
