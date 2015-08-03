use strict;
use warnings;

use AnyEvent::Hbase;
use Getopt::Long;
use Test::More;

use Data::Dumper;

my $host  = $ENV{'HBASE_THRIFT_HOST'}  || "127.0.0.1";
my $port  = $ENV{'HBASE_THRIFT_PORT'}  || 9090;
my $debug = $ENV{'HBASE_THRIFT_DEBUG'} || 0;

my $table_name = 'ae_hbase_test';

GetOptions(
    "host=s" => \$host,
    "port=i" => \$port,
    "debug"  => \$debug,
);

my $hbase = AnyEvent::Hbase->new(
    host    => $host . ':' . $port,
    timeout => 5,
    debug   => $debug,
);

my $cv = AnyEvent->condvar;

$cv->begin;
$hbase->connect(sub {
    my ( $status, $error ) = @_;
    ok( $status, "Connect" ) or BAIL_OUT( $_[1] );
    $cv->end;
});


$cv->begin;
$hbase->getTableNames(sub { 
    ok( $_[0], "Get tables names" ) or diag( $_[1] );
    my $tables = $_[1];

    unless ( grep { $_ eq $table_name } @{ $tables } ) {
        $hbase->createTable( $table_name, [ Hbase::ColumnDescriptor->new({ name => 'cf' }) ], sub {
            ok( $_[0], "Create table $table_name" ) or diag( $_[1] );
        })
    }

    $cv->end;
});

$cv->recv;

$cv = AnyEvent->condvar;

$cv->begin;
$hbase->mutateRow($table_name, 'row01', [ Hbase::Mutation->new({ column => 'cf:test', value => 'hello_world' }) ], sub {
    ok( $_[0], "Mutate single row" ) or diag( $_[1] );
    $cv->end;
});


my $batch_mutation = [
    Hbase::BatchMutation->new({
        row => 'row02',
        mutations => [
            Hbase::Mutation->new({ column => 'cf:int', value => 1    }),
            Hbase::Mutation->new({ column => 'cf:flt', value => 3.14 }),
        ]
    }),
    Hbase::BatchMutation->new({
        row => 'row03',
        mutations => [
            Hbase::Mutation->new({ column => 'cf:int', value => -1    }),
            Hbase::Mutation->new({ column => 'cf:flt', value => -3.14 }),
        ]
    }),
];

$cv->begin;
$hbase->mutateRows( $table_name, $batch_mutation, sub {
    ok( $_[0], "Mutate batch of rows" ) or diag( $_[1] );
    $cv->end;
});

$cv->begin;
$hbase->getRow( $table_name, 'row01', sub {
    ok( $_[0], "Get row" ) or diag( $_[1] );
    my $row = $_[1];
    is( $row->[0]->columns->{'cf:test'}->value, 'hello_world', "Check row value" );
    $cv->end;
});

$cv->begin;
$hbase->getRows( $table_name, [ 'row02', 'row03' ], sub {
    ok( $_[0], "Get rows" ) or diag( $_[1] );
    my $rows = $_[1];
    is( $rows->[1]->columns->{'cf:flt'}->value, -3.14, "Check row value" );
    $cv->end;
});


$cv->begin;
$hbase->scannerOpenWithScan( $table_name, Hbase::TScan->new({ startRow => 'row01', stopRow  => 'row10' } ), sub {
    my $scanner = $_[1] || return;
    my $all_rows;
    my $cb; $cb = sub {
        my $list = $_[1] || return;
        push( @$all_rows, @$list );
        @$list > 0
            ? $hbase->scannerGetList( $scanner, 1000, $cb )
            : $hbase->scannerClose( $scanner, sub {
                ok( 1, "Check scanner" );
                ok( ( grep { $_->row eq 'row02' } @$all_rows ), "Check scanner result" );
                $cv->end
            })
    };
    $hbase->scannerGetList( $scanner, 1000, $cb )
});


$cv->recv;

done_testing();


