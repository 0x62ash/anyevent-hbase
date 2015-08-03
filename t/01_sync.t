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

my ( $status, $error ) = $hbase->connect;
ok( $status, "Connect" );

if ( !$status ) {
    BAIL_OUT($error);
}

eval { $hbase->mutateRow( '___', [] ); };
ok( $@, "Test error handling" );


my $tables = eval { $hbase->getTableNames } or diag( Dumper($@) );
ok( $tables, "Get tables names" );


unless (grep { $_ eq $table_name } @{ $tables }) {
    eval { $hbase->createTable($table_name, [ Hbase::ColumnDescriptor->new({ name => 'cf' }) ]) };
    ok( !$@, "Create table $table_name" ) or diag( Dumper($@) );
}

eval { $hbase->mutateRow($table_name, 'row01', [ Hbase::Mutation->new({ column => 'cf:test', value => 'hello_world' }) ]) };
ok( !$@, "Mutate single row" ) or diag( Dumper($@) );


my $batch_mutation = [
    Hbase::BatchMutation->new({
        row => 'row02',
        mutations => [
            Hbase::Mutation->new({ column => 'cf:int', value => 1    }),
            Hbase::Mutation->new({ column => 'cf:flt', value => 3.14 })
        ]
    }),
    Hbase::BatchMutation->new({
        row => 'row03',
        mutations => [
            Hbase::Mutation->new({ column => 'cf:int', value => -1    }),
            Hbase::Mutation->new({ column => 'cf:flt', value => -3.14 })
        ]
    }),
];

eval { $hbase->mutateRows( $table_name, $batch_mutation ) };
ok( !$@, "Mutate batch of rows" ) or diag( Dumper($@) );


my $row = eval { $hbase->getRow( $table_name, 'row01' ) };
ok( !$@, "Get row" );
is( $row->[0]->columns->{'cf:test'}->value, 'hello_world', "Check row value" );


my $rows = eval { $hbase->getRows( $table_name, [ 'row02', 'row03' ] ) };
ok( !$@, "Get rows" );
is( $rows->[1]->columns->{'cf:flt'}->value, -3.14, "Check row value" );


my $all_rows;
eval {
    my $scanner = $hbase->scannerOpenWithStop( $table_name, 'row01', 'row10' );

    my $list;
    while ($list = $hbase->scannerGetList( $scanner, 1000 ) and @$list > 0 ) {
        push( @$all_rows, @$list );
    }

    $hbase->scannerClose($scanner);
};

ok( !$@, "Check scanner" );
ok( ( grep { $_->row eq 'row02' } @$all_rows ), "Check scanner result" );


$hbase->close;

done_testing();


