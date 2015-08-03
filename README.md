# AnyEvent::Hbase

AnyEvent::Hbase is asynchronous thrift API for HBase.

Internally it's wrapper around Hbase thrift library. API is the same, so you can easily migrate without breaking compatibility and use async calls only where you need it.


## Get started
- You will need to enable Framed transport on HBase Thrift server.

 hbase/conf/hbase-site.xml:

 ```xml
 <property>
     <name>hbase.regionserver.thrift.framed</name>
     <value>true</value>
 </property>
 ```

 > Description
 >
 > Use Thrift TFramedTransport on the server side. This is the recommended transport for thrift servers and requires a similar setting on the client side. Changing this to false will select the default transport, vulnerable to DoS when malformed requests are issued due to THRIFT-601.

- Install Thrift::XS from CPAN

 `cpanm Thrift::XS`

- Mantra commands

 `perl Makefile.PL && make && make install`

## Example

Synchronous 

```perl
my $hbase = AnyEvent::Hbase->new(
	host    => $host . ':' . $port,
	timeout => 5,
	debug   => 1,
);

my ( $status, $error ) = $hbase->connect;

my $tables = eval { $hbase->getTableNames };

if ($@) {
    print "Unable to getTableNames: " . ( ref($@) ? $@->{message} : $@ ) . "\n";
}

my $mutation = [ Hbase::Mutation->new({ column => 'cf:test', value => 'hello_world' }) ];

eval { $hbase->mutateRow( $table_name, 'row01', $mutation ) };

if ($@) {
    print "Unable to mutateRow: " . ( ref($@) ? $@->{message} : $@ ) . "\n";
}
```

Asynchronous

```perl
my $hbase = AnyEvent::Hbase->new(
	host    => $host . ':' . $port,
	timeout => 5,
	debug   => $debug,
);

my $cv = AnyEvent->condvar;
$cv->begin;

$hbase->connect(sub {
    my ( $status, $error ) = @_;
    print "Unable to connect: $error\n" unless $status;
    $cv->end;
});

my $tables;
$cv->begin;

$hbase->getTableNames(sub { 
    my ( $status, $result ) = @_;
    if ( $status ) {
        $tables = $result;
    } else {
        print "Unable to getTableNames: " . ( ref($result) ? $@->{message} : $result ) . "\n";
    }
    $cv->end;
});

my $mutation = [ Hbase::Mutation->new({ column => 'cf:test', value => 'hello_world' }) ];
$cv->begin;

$hbase->mutateRow($table_name, 'row01', $mutation, sub {
    my ( $status, $result ) = @_;
    unless ( $status ) {
        print "Unable to mutateRow: " . ( ref($result) ? $result->{message} : $result ) . "\n";
    }
    $cv->end;
});

$cv->recv;
```

For more examples, please check the included tests or read 
[Hbase thrift API documentation](http://htmlpreview.github.io/?https://raw.githubusercontent.com/0x62ash/anyevent-hbase/master/doc/Hbase/Hbase.html)

## Benchmark

This is quite dirty result from single node standalone HBase.

```text
Benchmark for mutateRow
                          Rate Hbase(Thrift) AnyEvent::Hbase Hbase(Thrift::XS) AnyEvent::Hbase(async)
Hbase(Thrift)           3546/s            --            -26%              -33%                   -76%
AnyEvent::Hbase         4808/s           36%              --               -9%                   -67%
Hbase(Thrift::XS)       5263/s           48%              9%                --                   -64%
AnyEvent::Hbase(async) 14706/s          315%            206%              179%                     --

Benchmark for getRow
                          Rate Hbase(Thrift) Hbase(Thrift::XS) AnyEvent::Hbase AnyEvent::Hbase(async)
Hbase(Thrift)           2551/s            --              -15%            -45%                   -89%
Hbase(Thrift::XS)       3012/s           18%                --            -35%                   -87%
AnyEvent::Hbase         4630/s           81%               54%              --                   -81%
AnyEvent::Hbase(async) 23810/s          833%              690%            414%                     --
```

## License
Based on Andy Grundman's Async Cassandra API
