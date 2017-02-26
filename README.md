# AnyEvent::Hbase

[![Kritika Status](https://kritika.io/users/0x62ash/repos/anyevent-hbase/heads/master/status.svg)](https://kritika.io/users/0x62ash/repos/anyevent-hbase/)

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

```text
Hbase(Thrift) mutateRow: 36.8895 wallclock secs ( 3.22 usr +  0.40 sys =  3.62 CPU) @ 1381.22/s (n=5000)
Hbase(Thrift) get: 24.4651 wallclock secs ( 5.68 usr +  0.40 sys =  6.08 CPU) @ 822.37/s (n=5000)
Hbase(Thrift::XS) mutateRow: 32.2815 wallclock secs ( 2.72 usr +  0.36 sys =  3.08 CPU) @ 1623.38/s (n=5000)
Hbase(Thrift::XS) get: 17.9919 wallclock secs ( 4.60 usr +  0.42 sys =  5.02 CPU) @ 996.02/s (n=5000)
AnyEvent::Hbase mutateRow: 29.6131 wallclock secs ( 2.60 usr +  0.41 sys =  3.01 CPU) @ 1661.13/s (n=5000)
AnyEvent::Hbase get: 16.8168 wallclock secs ( 3.01 usr +  0.43 sys =  3.44 CPU) @ 1453.49/s (n=5000)
AnyEvent::Hbase(async) mutateRow: 17.2016 wallclock secs ( 1.54 usr +  0.16 sys =  1.70 CPU) @ 2941.18/s (n=5000)
AnyEvent::Hbase(async) get: 4.84589 wallclock secs ( 2.05 usr +  0.10 sys =  2.15 CPU) @ 2325.58/s (n=5000)

Benchmark for mutateRow
                        Rate Hbase(Thrift) Hbase(Thrift::XS) AnyEvent::Hbase AnyEvent::Hbase(async)
Hbase(Thrift)          136/s            --              -12%            -20%                   -53%
Hbase(Thrift::XS)      155/s           14%                --             -8%                   -47%
AnyEvent::Hbase        169/s           25%                9%              --                   -42%
AnyEvent::Hbase(async) 291/s          114%               88%             72%                     --

Benchmark for getRow
                         Rate Hbase(Thrift) Hbase(Thrift::XS) AnyEvent::Hbase AnyEvent::Hbase(async)
Hbase(Thrift)           204/s            --              -26%            -31%                   -80%
Hbase(Thrift::XS)       278/s           36%                --             -7%                   -73%
AnyEvent::Hbase         297/s           45%                7%              --                   -71%
AnyEvent::Hbase(async) 1032/s          405%              271%            247%                     --
```

## License
Based on Andy Grundman's Async Cassandra API
