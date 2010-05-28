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
    return $schema->parse_struct($struct, $names);
}

sub parse_struct {
    my $schema = shift;
    my $struct = shift;
    my $names = shift || {};

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
use Scalar::Util;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = bless {}, $class;

    my $names  = $param{names} || {};
    my $struct = $param{struct} || {};
    my $name   = $schema->{fullname} = $struct->{name}
        or throw Avro::Schema::Error::ParseError( "Missing name for $class" );

    $class->add_name($names, $schema);
    return $schema;
}

sub add_name {
    my $class = shift;
    my ($names, $schema) = @_;

    my $name = $schema->fullname;
    if ( exists $names->{ $name } ) {
        throw Avro::Schema::Error::ParseError( "Name $name is already defined" );
    }
    $names->{$name} = $schema;
    Scalar::Util::weaken( $names->{$name} );
    return;
}

sub fullname {
    my $schema = shift;
    return $schema->{fullname};
}

sub name {
}

sub namespace {
}

package Avro::Schema::Record;
our @ISA = qw/Avro::Schema::Named/;
use Scalar::Util;

sub new {
    my $class = shift;
    my %param = @_;

    my $names  = $param{names} ||= {};
    my $schema = $class->SUPER::new(%param);

    my $fields = $param{struct}{fields}
        or throw Arvo::Schema::Error::ParseError("Record must have Fields");

    throw Arvo::Schema::Error::ParseError("Record.Fields must me an array")
        unless ref $fields eq 'ARRAY';

    my @fields;
    for my $field (@$fields) {
        my $name = $field->{name};
        throw Arvo::Schema::Error::ParseError("Record.Field.name is required")
            unless defined $name && length $name;

        my $type = $field->{type};
        throw Arvo::Schema::Error::ParseError("Record.Field.name is required")
            unless defined $type && length $type;

        $type = Avro::Schema->parse_struct($type, $names);
        my $field = { name => $name, type => $type };
        ## TODO: default
        Scalar::Util::weaken($field->{type});

        push @fields, $field;
    }
    $schema->{fields} = \@fields;
    return $schema;
}

sub fields {
    my $schema = shift;
    return $schema->{fields};
}

package Avro::Schema::Enum;
our @ISA = qw/Avro::Schema::Named/;

package Avro::Schema::Array;
our @ISA = qw/Avro::Schema::Base/;

package Avro::Schema::Map;
our @ISA = qw/Avro::Schema::Base/;

package Avro::Schema::Union;
our @ISA = qw/Avro::Schema::Base/;

sub parse_struct {
    my $schema = shift;
    my $types = shift;
    my $names = shift || {};
    for my $type (@$types) {
    }
}

package Avro::Schema::Fixed;
our @ISA = qw/Avro::Schema::Named/;

package Avro::Schema::Error::ParseError;
use parent 'Error::Simple';

1;
