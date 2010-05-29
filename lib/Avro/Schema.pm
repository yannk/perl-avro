package Avro::Schema;
use strict;
use warnings;

use Bit::Vector();
use JSON::XS();
use Try::Tiny;

my $json = JSON::XS->new->allow_nonref;

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

sub to_string {
    my $class = shift;
    my $struct = shift;
    return $json->encode($struct);
}

sub parse_struct {
    my $schema = shift;
    my $struct = shift;
    my $names = shift || {};

    ## 1.3.2 A JSON object
    if (ref $struct eq 'HASH') {
        my $type = $struct->{type}
            or throw Avro::Schema::Error::ParseError("type is missing");
        if ( Avro::Schema::Primitive->is_valid($type) ) {
            return Avro::Schema::Primitive->new(type => $type);
        }
        if ($type eq 'record') {
            return Avro::Schema::Record->new(
                struct => $struct,
                names => $names,
            );
        }
        elsif ($type eq 'enum') {
            return Avro::Schema::Enum->new(struct => $struct, names => $names);
        }
        elsif ($type eq 'array') {
            return Avro::Schema::Array->new(
                struct => $struct,
                names => $names,
            );
        }
        elsif ($type eq 'map') {
            return Avro::Schema::Map->new(
                struct => $struct,
                names => $names,
            );
        }
        elsif ($type eq 'fixed') {
            return Avro::Schema::Fixed->new(
                struct => $struct,
                names => $names,
            );
        }
        else {
            throw Avro::Schema::Error::ParseError("unknown type: $type");
        }
    }
    ## 1.3.2 A JSON array, representing a union of embedded types.
    elsif (ref $struct eq 'ARRAY') {
        return Avro::Schema::Union->new(struct => $struct, names => $names);
    }
    ## 1.3.2 A JSON string, naming a defined type.
    else {
        my $type = $struct;
        ## It's one of our custom defined type
        if (exists $names->{$type}) {
            return $names->{$type};
        }
        ## It's a primitive type
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
    return $schema;
}

sub type {
    my $schema = shift;
    return $schema->{type};
}

sub to_string {
    my $schema = shift;
    my $known_names = shift || {};
    return Avro::Schema->to_string($schema->to_struct($known_names));
}

package Avro::Schema::Primitive;
our @ISA = qw/Avro::Schema::Base/;
use Carp;
use Regexp::Common qw/number/;

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

    throw Avro::Schema::Error::ParseError("Not a primitive type $type")
        unless $class->is_valid($type);

    if (! exists $Singleton{ $type } ) {
        my $schema = $class->SUPER::new( type => $type );
        $Singleton{ $type } = $schema;
    }
    return $Singleton{ $type };
}

sub is_valid {
    return $PrimitiveType{ $_[1] || "" };
}

## takes a default value and return an array:
## ($bool, $normalized) where $bool is true if the default value is valid
## and $normalized is the normalized default value to use
sub is_default_valid {
    my $schema = shift;
    my $default = shift;
    my $type = $schema->{type};
    if ($type eq 'null') {
        return defined $default ? (0) : (1, undef);
    }
    if ($type eq 'boolean') {
        return (1, $default ? 1 : 0);
    }
    unless (defined $default && length $default) {
        return (0);
    }
    if ($type eq "bytes" or $type eq "string") {
        return (1, $default);
    }
    if ($type eq 'int') {
        no warnings;
        my $packed_int = pack "l", $default;
        my $unpacked_int = unpack "l", $packed_int;
        return $unpacked_int eq $default ? (1, $default) : (0);
    }
    if ($type eq 'long') {
        ## TODO: faster with pack Q if 64bit perl
        my $dec;
        eval {
            my $vec = Bit::Vector->new_Dec(64, $default);
            $dec = $vec->to_Dec;
        };
        return (0) if $@;
        return ($dec eq $default) ? (1, $default) : (0);
    }
    if ($type eq 'float' or $type eq 'double') {
        $default =~ /^$RE{num}{real}$/ ? return (1, $default) : (0);
    }
}

sub to_struct {
    my $schema = shift;
    return $schema->type;
}

package Avro::Schema::Named;
our @ISA = qw/Avro::Schema::Base/;
use Scalar::Util;

my %NamedType = map { $_ => 1 } qw/
    record
    enum
    fixed
/;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = $class->SUPER::new(%param);

    my $names  = $param{names} || {};
    my $struct = $param{struct} || {};
    my $name   = $schema->{fullname} = $struct->{name}
        or throw Avro::Schema::Error::ParseError( "Missing name for $class" );

    $class->add_name($names, $schema);
    return $schema;
}

sub is_valid {
    return $NamedType{ $_[1] || "" };
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

my %ValidOrder = map { $_ => 1 } qw/ascending descending ignore/;

sub new {
    my $class = shift;
    my %param = @_;

    my $names  = $param{names} ||= {};
    my $schema = $class->SUPER::new(%param);

    my $fields = $param{struct}{fields}
        or throw Avro::Schema::Error::ParseError("Record must have Fields");

    throw Avro::Schema::Error::ParseError("Record.Fields must me an array")
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
        my $f = { name => $name, type => $type };
        #TODO: find where to weaken precisely
        #Scalar::Util::weaken($field->{type});

        if (exists $field->{default}) {
            my ($is_valid, $default) =
                $type->is_default_valid($field->{default});
            my $t = $type->type;
            throw Avro::Schema::Error::ParseError(
                "default value doesn't validate $t: '$field->{default}'"
            ) unless $is_valid;
            $f->{default} = $default;
        }
        if (my $order = $field->{order}) {
            throw Avro::Schema::Error::ParseError(
                "Order '$order' is not valid'"
            ) unless $ValidOrder{$order};
            $f->{order} = $order;
        }

        push @fields, $f;
    }
    $schema->{fields} = \@fields;
    return $schema;
}

sub field_to_struct {
    my $field = shift;
    my $known_names = shift || {};
    my $type = $field->{type}->to_struct($known_names);
    return { name => $field->{name}, type => $type };
}

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};
    ## consider that this record type is now known (will serialize differently)
    my $fullname = $schema->fullname;
    if ($known_names->{ $fullname }++) {
        return $fullname;
    }
    return {
        type => 'record',
        name => $fullname,
        fields => [
            map { field_to_struct($_, $known_names) } @{ $schema->{fields} }
        ],
    };
}

sub fields {
    my $schema = shift;
    return $schema->{fields};
}

package Avro::Schema::Enum;
our @ISA = qw/Avro::Schema::Named/;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = $class->SUPER::new(%param);
    my $struct = $param{struct}
        or throw Avro::Schema::Error::ParseError("Enum instantiation");
    my $symbols = $struct->{symbols} || [];

    unless (@$symbols) {
        throw Avro::Schema::Error::ParseError("Enum needs at least one symbol");
    }
    my %symbols;
    for (@$symbols) {
        if (ref $_) {
            throw Avro::Schema::Error::ParseError(
                "Enum.symbol should be a string"
            );
        }
        throw Avro::Schema::Error::ParseError("Duplicate symbol in Enum")
            if $symbols{$_}++;
    }
    $schema->{symbols} = \%symbols;
    return $schema;
}

sub is_default_valid {
    my $schema = shift;
    my $default = shift;
    return (1, $default) if defined $default && $schema->{symbols}{$default};
    return (0);
}

sub symbols {
    my $schema = shift;
    return [ keys %{ $schema->{symbols} } ];
}

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};

    my $fullname = $schema->fullname;
    if ($known_names->{ $fullname }++) {
        return $fullname;
    }
    return {
        type => 'enum',
        name => $schema->fullname,
        symbols => [ keys %{ $schema->{symbols} } ],
    };
}

package Avro::Schema::Array;
our @ISA = qw/Avro::Schema::Base/;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = $class->SUPER::new(%param);

    my $struct = $param{struct}
        or throw Avro::Schema::Error::ParseError("Enum instantiation");

    my $items = $struct->{items}
        or throw Avro::Schema::Error::ParseError("Array must declare 'items'");

    unless (defined $items && length $items) {
        throw Avro::Schema::Error::ParseError(
            "Array.items should be a string"
        );
    }
    $schema->{items} = $items;
    return $schema;
}

sub is_default_valid {
    my $schema = shift;
    my $default = shift;
    return (1, $default) if $default && ref $default eq 'ARRAY';
    return (0);
}

sub items {
    my $schema = shift;
    return $schema->{items};
}

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};

    return {
        type => 'array',
        items => $schema->{items},
    };
}

package Avro::Schema::Map;
our @ISA = qw/Avro::Schema::Base/;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = $class->SUPER::new(%param);

    my $struct = $param{struct}
        or throw Avro::Schema::Error::ParseError("Map instantiation");

    my $values = $struct->{values};
    unless (defined $values && length $values) {
        throw Avro::Schema::Error::ParseError("Map must declare 'values'");
    }
    if (ref $values) {
        throw Avro::Schema::Error::ParseError(
            "Map.values should be a string"
        );
    }
    $schema->{values} = $values;

    return $schema;
}

sub is_default_valid {
    my $schema = shift;
    my $default = shift;
    return (1, $default) if $default && ref $default eq 'HASH';
    return (0);
}

sub values {
    my $schema = shift;
    return $schema->{values};
}

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};

    return {
        type => 'map',
        values => $schema->{values},
    };
}

package Avro::Schema::Union;
our @ISA = qw/Avro::Schema::Base/;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = $class->SUPER::new(%param);
    my $union = $param{struct}
        or throw Avro::Schema::Error::ParseError("Union.new needs a struct");

    my $names = $param{names} ||= {};
    my @schemas;
    my %seen_types;
    for my $struct (@$union) {
        my $sch = Avro::Schema->parse_struct($struct, $names);
        my $type = $sch->type;

        ## 1.3.2 Unions may not contain more than one schema with the same
        ## type, except for the named types record, fixed and enum. For
        ## example, unions containing two array types or two map types are not
        ## permitted, but two types with different names are permitted.
        if (Avro::Schema::Named->is_valid($type)) {
            $type = $sch->fullname; # resolve Named types to their name
        }
        ## XXX: I could define &type_name doing the correct resolution for all classes
        if ($seen_types{ $type }++) {
            throw Avro::Schema::Error::ParseError(
                "$type is present more than once in the union"
            )
        }
        ## 1.3.2 Unions may not immediately contain other unions.
        if ($type eq 'union') {
            throw Avro::Schema::Error::ParseError(
                "Cannot embed unions in union"
            );
        }
        push @schemas, $sch;
    }
    $schema->{schemas} = \@schemas;

    return $schema;
}

sub is_default_valid { (0) }

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};
    return [ map { $_->to_struct($known_names) } @{$schema->{schemas}} ];
}

package Avro::Schema::Fixed;
our @ISA = qw/Avro::Schema::Named/;

sub new {
    my $class = shift;
    my %param = @_;
    my $schema = $class->SUPER::new(%param);

    my $struct = $param{struct}
        or throw Avro::Schema::Error::ParseError("Fixed instantiation");

    my $size = $struct->{size};
    unless (defined $size && length $size) {
        throw Avro::Schema::Error::ParseError("Fixed must declare 'size'");
    }
    if (ref $size) {
        throw Avro::Schema::Error::ParseError(
            "Fixed.size should be a scalar"
        );
    }
    unless ($size =~ m{^\d+$} && $size > 0) {
        throw Avro::Schema::Error::ParseError(
            "Fixed.size should be a positive integer"
        );
    }
    $schema->{size} = $size;

    return $schema;
}

sub is_default_valid {
    my $schema = shift;
    my $default = shift;
    my $size = $schema->{size};
    return (1, $default) if $default && bytes::length $default == $size;
    return (0);
}

sub size {
    my $schema = shift;
    return $schema->{size};
}

sub to_struct {
    my $schema = shift;
    my $known_names = shift || {};

    my $fullname = $schema->fullname;
    if ($known_names->{ $fullname }++) {
        return $fullname;
    }

    return {
        type => 'fixed',
        name => $fullname,
        size => $schema->{size},
    };
}


package Avro::Schema::Error::ParseError;
use parent 'Error::Simple';

1;
