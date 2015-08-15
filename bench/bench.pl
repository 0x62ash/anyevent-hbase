#!/usr/bin/perl

use strict;
use warnings;

use AnyEvent::Hbase;
use Thrift::Socket;
use Thrift::FramedTransport;
use Thrift::BinaryProtocol;
use Thrift::XS;

use Benchmark qw(:all :hireswallclock);

use Data::Dumper;

my $host = $ARGV[0] || "127.0.0.1";
my $port = $ARGV[1] || 9090;
my $table_name_prefix = 'bench';
my $buffer_size = 8192;
my $timeout = 15;
my $transport; # Thrift::XS::BinaryProtocol needs global variable for Thrift::FramedTransport transport. Dont ask me why...

sub _connect {
    my ($type, $host, $port) = @_;

    if ($type eq 'Hbase(Thrift)') {
        my $socket = Thrift::Socket->new($host, $port);
        $socket->setSendTimeout ($timeout*1000);
        $socket->setRecvTimeout ($timeout*1000);

        my $transport = Thrift::FramedTransport->new($socket, $buffer_size, $buffer_size);
        my $protocol  = Thrift::BinaryProtocol->new($transport);
        my $hbase     = Hbase::HbaseClient->new($protocol);

        eval {
            $transport->open();
        };

        if ($@) {
            warn "Thrift: unable to connect: ".( ref($@) ? $@->{message} : $@ ).".\n";
            return;
        }

        $hbase->{transport} = $transport;
        return $hbase;
    }
    elsif ($type eq 'Hbase(Thrift::XS)') {
        my $socket = Thrift::Socket->new($host, $port);
        $socket->setSendTimeout ($timeout*1000);
        $socket->setRecvTimeout ($timeout*1000);

        $transport   = Thrift::FramedTransport->new($socket, $buffer_size, $buffer_size);
        my $protocol = Thrift::XS::BinaryProtocol->new($transport);
        my $hbase    = Hbase::HbaseClient->new($protocol);

        eval {
            $transport->open();
        };

        if ($@) {
            warn "Thrift (XS): unable to connect: ".( ref($@) ? $@->{message} : $@ ).".\n";
            return;
        }

        $hbase->{transport} = $transport;
        return $hbase;
    }
    elsif ($type =~ m/AnyEvent::Hbase/) {
        my $hbase = AnyEvent::Hbase->new(
            host        => $host . ':' . $port,
            timeout     => $timeout,
            buffer_size => $buffer_size,
            #debug       => 1,
        );

        my ( $status, $error ) = $hbase->connect->recv;
        unless ($status) {
            warn "AnyEvent::Hbase: unable to connect: $error\n";
            return;
        }

        return $hbase;
    }
}

sub _disconnect {
    my ($type, $hbase) = @_;
    if ($type =~ m/^Hbase/) {
        $hbase->{transport}->close;
    }
    elsif ($type =~ m/^AnyEvent::Hbase/) {
        $hbase->close;
    }
    else {
        die "Something wrong\n";
    }
}


my (%mut, %get);
my $count = 1000;
my $bigstr; $bigstr .= ('a'..'z', 'A'..'Z', 0..9)[rand 62] for (1..512);
my $key_fmt = 'foooo:baaaaar:iter:%010d'; # fixed len keys

my $mutations = [
    Hbase::Mutation->new({ column => 'cf:bigstr', value => $bigstr   }),
    Hbase::Mutation->new({ column => 'cf:int1',   value => 987654321 }),
    Hbase::Mutation->new({ column => 'cf:int2',   value => 123456789 }),
];

eval {
    for ( qw[ Hbase(Thrift) Hbase(Thrift::XS) AnyEvent::Hbase AnyEvent::Hbase(async) ] ) {
        my $table_name = sprintf("%-30s", $table_name_prefix . $_); $table_name =~ s/\W/_/g; # fixed len table names
        print "test $_\n";

        my $hbase = _connect($_, $host, $port);
        next unless $hbase;

        $hbase->createTable($table_name, [ Hbase::ColumnDescriptor->new({ name => 'cf' }) ])
            unless grep { $_ eq $table_name } @{ $hbase->getTableNames };

        my $t0;
        if ($_ =~ m/async/) {
            my $cv;
            $t0 = Benchmark->new;
                $cv = AnyEvent->condvar;
                for my $i (1..$count) {
                    $cv->begin;
                    $hbase->mutateRow($table_name, sprintf($key_fmt, $i), $mutations, sub { warn $_[1] unless $_[0]; $cv->end });
                }
                $cv->recv;
            $mut{$_} = timediff(Benchmark->new, $t0);
            $t0 = Benchmark->new;
                $cv = AnyEvent->condvar;
                for my $i (1..$count) {
                    $cv->begin;
                    $hbase->getRow($table_name, sprintf($key_fmt, $i), sub { warn $_[1] unless $_[0]; $cv->end });
                }
                $cv->recv;
            $get{$_} = timediff(Benchmark->new, $t0);
        } else {
            $t0 = Benchmark->new;
                for my $i (1..$count) { $hbase->mutateRow($table_name, sprintf($key_fmt, $i), $mutations); }
            $mut{$_} = timediff(Benchmark->new, $t0);
            $t0 = Benchmark->new;
                for my $i (1..$count) { $hbase->getRow($table_name, sprintf($key_fmt, $i)); }
            $get{$_} = timediff(Benchmark->new, $t0);
        }

        # we want real counts in timestr
        $mut{$_}->[-1] = $get{$_}->[-1] = $count;
        print "$_ mutateRow: ",timestr($mut{$_}),"\n";
        print "$_ get: ",timestr($get{$_}),"\n";

        # cleanup

        $hbase->disableTable( $table_name );
        $hbase->deleteTable( $table_name );

        _disconnect($_, $hbase);
    }
};
die "Got error: ".Dumper($@) if $@;

# because we measure over network, we want operations per wallclock, not usr+sys
@{ $mut{$_} }[1,2] = ($mut{$_}->[0], 0) for keys %mut;
@{ $get{$_} }[1,2] = ($get{$_}->[0], 0) for keys %get; 

print "\nBenchmark for mutateRow\n";
cmpthese(\%mut);
print "\nBenchmark for getRow\n";
cmpthese(\%get);
