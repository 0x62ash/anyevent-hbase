package AnyEvent::Hbase;

use strict;
use warnings;

use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Hbase::Types;
use AnyEvent::Hbase::Hbase;
use Thrift::XS;
use Carp qw(croak);

use Time::HiRes;

our $VERSION = '0.01';
#our @CARP_NOT;
#@CARP_NOT = qw(AnyEvent::Loop AnyEvent::CondVar::Base);
$Carp::CarpInternal{'AnyEvent::Loop'}++;
$Carp::CarpInternal{'AnyEvent::CondVar::Base'}++;


sub AUTOLOAD {
    my ( $self, @opts ) = @_;
    my $me = our $AUTOLOAD;
    $me =~ s/.*:://;
    #warn "AUTOLOAD $me\n" if $self->{debug};

    no strict 'refs';

    *{$me} = sub {
        my ( $self, @opts ) = @_;
        my $cb;
        if ( ref( $opts[-1] ) eq 'CODE' ) {
            $cb = pop( @opts );
        }
        return $self->_call( $me, [ @opts ], $cb );
    };
    
    return *{$me}->($self, @opts);
}

sub new {
    my ( $class, %opts ) = @_;
    
    die 'host is required' unless $opts{host};
    
    if ( !ref $opts{host} ) {
        $opts{host} = [ $opts{host} ];
    }
    
    $opts{hosts} = [ map { [ split /:/ ] } @{ $opts{host} } ];
    
    my $self = bless {
        hosts          => $opts{hosts},
        connected      => 0,
        auto_reconnect => exists $opts{auto_reconnect} ? $opts{auto_reconnect} : 1,
        max_retries    => exists $opts{max_retries} ? $opts{max_retries} : 1,
        buffer_size    => $opts{buffer_size} || 8192,
        timeout        => $opts{timeout}     || 30,
        debug          => $opts{debug}       || 0,
    }, $class;
    
    $self->{transport} = Thrift::XS::MemoryBuffer->new( $self->{buffer_size} );
    $self->{protocol}  = Thrift::XS::BinaryProtocol->new( $self->{transport} );
    $self->{api}       = Hbase::HbaseClient->new( $self->{protocol} );
    
    return $self;
}

sub connect {
    my ( $self, $cb ) = @_;
    
    $cb ||= AnyEvent->condvar;
    
    my $t = AnyEvent->timer(
        after => $self->{timeout},
        cb    => sub {
            warn "<< connect [TIMEOUT]\n" if $self->{debug};
            $cb->( 0, 'Connect timed out' );
            $self->{handle}->destroy if $self->{handle};
        }
    );
    
    my $ts;
    my $host = @{ $self->{hosts} }[ rand $#{$self->{hosts}} ];
    $self->{debug} && ($ts = Time::HiRes::time()) && warn ">> connect to $host->[0]:$host->[1]\n";
    
    $self->{connected} = 2; # connecting...

    $self->{handle} = AnyEvent::Handle->new(
        connect    => $host,
        keepalive  => 1,
        no_delay   => 1,
        on_connect => sub {
            warn "<< connected (" . sprintf("%.1f", (Time::HiRes::time() - $ts) * 1000) . " ms)\n" if $self->{debug};
            
            undef $t;
            $self->{connected} = 1;

            $cb->( 1 );
        },
        on_error   => sub {
            warn "<< connect [ERROR] $_[2]\n" if $self->{debug};
            
            undef $t;
            $self->{connected} = 0;
            
            $cb->( 0, $_[2] );
            $_[0]->destroy;
            delete $self->{handle};
        },
        on_read    => sub { },
    );
    
    return wantarray ? ( $cb->recv ) : $cb;
}

sub close {
    my $self = shift;
    
    if ( defined $self->{handle} ) {
        warn ">> close\n" if $self->{debug};
        $self->{handle}->push_shutdown;
        $self->{connected} = 0;
    }
}

sub _error {
    my ( $self, $error, $cb ) = @_;
    ref( $cb ) eq 'CODE' ? $cb->( 0, $error ) : croak( $error );
}

sub _call {
    my ( $self, $method, $args, $cb, $retry ) = @_;

    # Methods without any args may pass callback only
    if ( ref $args->[0] eq 'CODE' ) {
        $cb = $args->[0];
        $args = [];
    }
    
    if ( !$self->{connected} ) {
        if ( $self->{auto_reconnect} ) {
            $retry ||= 0;
            
            if ( $retry > $self->{max_retries} ) {
                warn "<< max_retries reached, unable to auto-reconnect\n" if $self->{debug};
                $self->_error("max_retries reached, unable to auto-reconnect", $cb);
            }
            
            warn ">> $method [NOT CONNECTED] will auto-reconnect\n" if $self->{debug};
            $self->connect( sub {
                my ( $ok, $error ) = @_;
                if ( !$ok ) {
                    warn "<< auto-reconnect [ERROR] $error\n" if $self->{debug};
                    $self->_error("auto-reconnect failed ($error)", $cb);
                }
                else {
                    # Retry the call
                    ++$retry;
                    $self->_call( $method, $args, $cb, $retry );
                }
            } );
        }
        else {
            warn ">> $method [ERROR] not connected\n" if $self->{debug};
            $self->_error("not connected, you may want to enable auto_reconnect => 1", $cb);
        }
        
        return;
    }
    
    my $ts;
    ($ts = Time::HiRes::time()) && warn ">> $method " . ( $retry ? "[RETRY $retry]" : "" ) . "\n" if $self->{debug};
    
    my $handle = $self->{handle};
    my $membuf = $self->{transport};
    
    my $send = "send_${method}";
    my $recv = "recv_${method}";
    
    unless ( $cb ) {
        $cb ||= AnyEvent->condvar;
    }

    my $t = AnyEvent->timer(
        after => $self->{timeout},
        cb    => sub {
            warn "<< $method [TIMEOUT]\n" if $self->{debug};
            $self->_error("Request $method timed out", $cb);
        }
    );

    $self->{api}->$send( @{$args} );
    
    # Write in framed format
    my $len = $membuf->available();
    $handle->push_write( pack('N', $len) . $membuf->read($len) );
    
    # Read frame length
    $handle->push_read( chunk => 4, sub {
        my $len = unpack 'N', $_[1];
        
        # Read frame data
        $handle->unshift_read( chunk => $len, sub {
            undef $t;
            
            $membuf->write($_[1]);
            
            my $result = eval { $self->{api}->$recv() };

            if ( $@ ) {
                if ( $self->{debug} ) {
                    require Data::Dumper;
                    local $Data::Dumper::Indent = 0;
                    warn "<< $method [ERROR] " . Data::Dumper::Dumper($@) . "\n";
                };
                #$self->_error($@, $cb);
                $cb->( 0, $@ );
            }
            else {
                warn "<< $method OK (" . sprintf("%.1f", (Time::HiRes::time() - $ts) * 1000) . " ms)\n" if $self->{debug};
                $cb->( 1, $result );
            }
        } );
    } );
    
    #if ($sync) {
    #    my ($status, $result) = $cb->recv;
    #    return $status ? $result : croak $result;
    #} else {
    #    return $cb;
    #}
    if (ref $cb ne 'CODE') {
        my ($status, $result) = $cb->recv;
        return $status ? $result : $self->_error( $result );
    }
    else {
        return $cb;
    }
}

1;
