use strict;

use ExtUtils::MakeMaker qw(WriteMakefile);

WriteMakefile(
    NAME              => 'AnyEvent::Hbase',
    VERSION_FROM      => 'lib/AnyEvent/Hbase.pm',
    PREREQ_PM         => {
        'AnyEvent'        => 0,
        'Data::Dumper'    => 0,
        'Thrift::XS'      => 1.04,
        'Time::HiRes'     => 0,
    },
    #ABSTRACT_FROM     => 'lib/AnyEvent/Hbase.pm',
    ABSTRACT          => 'AnyEvent HBase thrift API',
    AUTHOR            => '<0x62ash@gmail.com>',
    META_MERGE        => {
        resources => {
            #license    => 'http://www.apache.org/licenses/LICENSE-2.0',
            repository => 'https://github.com/0x62ash/anyevent-hbase',
        },
    },
);
