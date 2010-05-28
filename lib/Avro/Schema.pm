package Avro::Schema;
use strict;
use warnings;

use JSON::XS;
use Try::Tiny;

my $json = JSON::XS->new->allow_nonref(1);

sub parse {
    my $schema      = shift;
    my $json_string = shift;
    my $names       = shift || {};

    my $struct = try {
        $json->decode($json_string);
    }
    catch {
        throw Avro::Schema::Error::ParseError(
            "Cannot parse json string: $_"
        );
    };

    ## A JSON object
    if (ref $struct eq 'HASH') {
        my $type = $struct->{type};
        if ( Avro::Schema::Primitive->is_valid($type) ) {
            return Avro::Schema::Primitive->new(type => $type);
        }
        return Avro::Schema::Record->new(struct => $struct, names => $names);
    }
    ## A JSON array, representing a union of embedded types.
    elsif (ref $struct eq 'ARRAY') {
        return Avro::Schema::Union->new(struct => $struct, names => $names);
    }
    ## A JSON string, naming a defined type.
    else {
        my $type = $struct;
        if (exists $names->{$type}) {
            return $names->{$type};
        }
        else {
            return Avro::Schema::Primitive->new(type => $type);
        }
    }
}

sub _validate_type {
    my $type = shift;
    my $names = shift || {};
    return if exists $names->{$type}
                  or Avro::Schema::Primitive->is_valid($type);

    throw Avro::Schema::Error::ParseError( "Invalid type: $type");
}

package Avro::Schema::Base;
use Carp;

sub new {
    my $class = shift;
    my %param = @_;

    my $type = $param{type};
    if (!$type) {
        my ($t) = $class =~ /::([^:]+)$/;
        $type = lc ($t);
    }
    my $schema = bless {
        type => $type,
    }, $class;
#    $schema->parse($struct);
    return $schema;
}

sub type {
    my $schema = shift;
    return $schema->{type};
}

package Avro::Schema::Primitive;
our @ISA = qw/Avro::Schema::Base/;
use Carp;

my %PrimitiveType = map { $_ => 1 } qw/
    null
    boolean
    int
    long
    float
    double
    bytes
    string
/;

my %Singleton = ( );

## FIXME: useless lazy generation
sub new {
    my $class = shift;
    my %param = @_;

    my $type = $param{type}
        or croak "Schema must have a type";

    if (! exists $Singleton{ $type } ) {
        my $schema = $class->SUPER::new( type => $type );
        $Singleton{ $type } = $schema;
    }
    return $Singleton{ $type };
}

sub is_valid {
    return $PrimitiveType{ $_[1] || "" };
}

package Avro::Schema::Named;
our @ISA = qw/Avro::Schema::Base/;

package Avro::Schema::Record;
our @ISA = qw/Avro::Schema::Named/;

package Avro::Schema::Enum;
our @ISA = qw/Avro::Schema::Named/;

package Avro::Schema::Array;
our @ISA = qw/Avro::Schema::Base/;

package Avro::Schema::Map;
our @ISA = qw/Avro::Schema::Base/;

package Avro::Schema::Union;
our @ISA = qw/Avro::Schema::Base/;

sub parse {
    my $schema = shift;
    my $types = shift;
    my $names = shift || {};
    for my $type (@$types) {
        Avro::Schema::_validate_type($type, $names);
    }
}

package Avro::Schema::Fixed;
our @ISA = qw/Avro::Schema::Named/;

package Avro::Schema::Error::ParseError;
use parent 'Error::Simple';

1;
