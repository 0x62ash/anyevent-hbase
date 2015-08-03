#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib';

use AnyEvent::Hbase;
use Thrift::Socket;
use Thrift::FramedTransport;
use Thrift::BinaryProtocol;
use Thrift::XS;

use Benchmark qw(:all);

use Data::Dumper;

my $host = $ARGV[0] || "127.0.0.1";
my $port = $ARGV[1] || 9090;
my $table_name_prefix = 'bench';
my $buffer_size = 8192;
my $timeout = 3;
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
        }

        $hbase->{transport} = $transport;
        return $hbase;
    }
    elsif ($type =~ m/AnyEvent::Hbase/) {
        my $hbase = AnyEvent::Hbase->new(
            host        => $host . ':' . $port,
            timeout     => $timeout,
            buffer_size => $buffer_size,
            #debug      => 1,
        );

        $hbase->connect->recv;

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


my %t_mut;
my %t_get;
my $count = 5_000;
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

        $hbase->createTable($table_name, [ Hbase::ColumnDescriptor->new({ name => 'cf' }) ])
            unless grep { $_ eq $table_name } @{ $hbase->getTableNames };

        my $iter = 0;
        if ($_ =~ m/async/) {
            my $cv = AnyEvent->condvar;
            $t_mut{$_} = timethis($count, sub { $cv->begin; $hbase->mutateRow($table_name, sprintf($key_fmt, $iter), $mutations, sub { $cv->end }); $iter++ }, $_." mutate");
            $cv->recv;
            $iter = 0;
            $cv = AnyEvent->condvar;
            $t_get{$_} = timethis($count, sub { $cv->begin; $hbase->getRow($table_name, sprintf($key_fmt, $iter), sub { $cv->end }); $iter++ }, $_." get");
            $cv->recv;
        } else {
            $t_mut{$_} = timethis($count, sub { $hbase->mutateRow($table_name, sprintf($key_fmt, $iter), $mutations); $iter++ }, $_." mutate");
            $iter = 0;
            $t_get{$_} = timethis($count, sub { $hbase->getRow($table_name, sprintf($key_fmt, $iter)); $iter++ }, $_." get");
        }

        # cleanup

        $hbase->disableTable( $table_name );
        $hbase->deleteTable( $table_name );

        _disconnect($_, $hbase);
    }
};
die "Got error: ".Dumper($@) if $@;

print "\nBenchmark for mutateRow\n";
cmpthese(\%t_mut);
print "\nBenchmark for getRow\n";
cmpthese(\%t_get);
