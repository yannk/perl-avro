package Avro::BinaryDecoder;
use strict;
use warnings;

use Config;
use Encode();
use Error::Simple;

our $complement = ~0x7F;
unless ($Config{use64bitint}) {
    require Math::BigInt;
    $complement = Math::BigInt->new("0b" . ("1" x 57) . ("0" x 7));
}

=head2 decode(%param)

Resolve the given writer and reader_schema to decode the data provided by the
reader.

=over 4

=item * writer_schema

The schema that was used to encode the data provided by the C<reader>

=item * reader_schema

The schema we want to use to decode the data.

=item * reader

An object implementing a straightforward interface. C<read($buf, $nbytes)> and
C<seek($nbytes, $whence)> are expected. Typically a IO::String object or a
IO::File object. It is expected that this calls will block the decoder, if not
enough data is available for read.

=back

=cut
sub decode {
    my $class = shift;
    my %param = @_;

    my ($writer_schema, $reader_schema, $reader)
        = @param{qw/writer_schema reader_schema reader/};

    ## a schema can also be just a string
    my $wtype = ref $writer_schema ? $writer_schema->type : $writer_schema;
    #my $rtype = ref $reader_schema ? $reader_schema->type : $reader_schema;

    resolve_schema($writer_schema, $reader_schema);

    my $meth = "decode_$wtype";
    return $class->$meth($writer_schema, $reader_schema, $reader);
}

sub resolve_schema {
}

sub decode_null { undef }

sub decode_boolean {
    my $class = shift;
    my $reader = pop;
    $reader->read(my $bool, 1);
    return $bool ? 1 : 0;
}

sub decode_int {
    my $class = shift;
    my $reader = pop;
    return zigzag(unsigned_varint($reader));
}

sub decode_long {
    my $class = shift;
    return decode_int($class, @_);
}

sub decode_float {
    my $class = shift;
    my $reader = pop;
    $reader->read(my $buf, 4);
    return unpack "f<", $buf;
}

sub decode_double {
    my $class = shift;
    my $reader = pop;
    $reader->read(my $buf, 8);
    return pack "d<", $buf,
}

sub decode_bytes {
    my $class = shift;
    my $reader = pop;
    my $size = decode_long($class, undef, undef, $reader);
    $reader->read(my $buf, $size);
    return $buf;
}

sub decode_string {
    my $class = shift;
    my $reader = pop;
    my $bytes = decode_bytes($class, undef, undef, $reader);
    return Encode::decode_utf8($bytes);
}

## 1.3.2 A record is encoded by encoding the values of its fields in the order
## that they are declared. In other words, a record is encoded as just the
## concatenation of the encodings of its fields. Field values are encoded per
## their schema.
sub decode_record {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $record;
    for my $field (@{ $writer_schema->fields }) {
        ## TODO: schema resolution
        my $field_schema = $field->{type};
        my $data = $class->decode(
            writer_schema => $field_schema,
            reader_schema => $field_schema,
            reader        => $reader,
        );
        $record->{ $field->{name} } = $data;
    }
    ## TODO: default values. (grep)
    return $record;
}

## 1.3.2 An enum is encoded by a int, representing the zero-based position of
## the symbol in the schema.
sub decode_enum {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $index = decode_int($class, @_);

    my $symbols = $writer_schema->symbols;
    my $enum_schema = $symbols->[$index];
    my $data = $class->decode(
        writer_schema => $enum_schema,
        reader_schema => $enum_schema,
        reader        => $reader,
    );
    return $data;
}

## 1.3.2 Arrays are encoded as a series of blocks. Each block consists of a
## long count value, followed by that many array items. A block with count zero
## indicates the end of the array. Each item is encoded per the array's item
## schema.
## If a block's count is negative, its absolute value is used, and the count is
## followed immediately by a long block size
sub decode_array {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $block_count = decode_long($class, @_);
    my @array;
    while ($block_count) {
        my $block_size;
        if ($block_count < 0) {
            $block_count = -$block_count;
            $block_size = decode_long($class, @_);
            ## XXX we can skip with $reader_schema?
        }
        for (1..$block_count) {
            push @array, $class->decode(
                writer_schema => $writer_schema->items,
                reader_schema => $reader_schema->items,
                reader        => $reader,
            );
        }
        $block_count = decode_long($class, @_);
    }
    return \@array;
}


## 1.3.2 Maps are encoded as a series of blocks. Each block consists of a long
## count value, followed by that many key/value pairs. A block with count zero
## indicates the end of the map. Each item is encoded per the map's value
## schema.
##
## If a block's count is negative, its absolute value is used, and the count is
## followed immediately by a long block size indicating the number of bytes in
## the block. This block size permits fast skipping through data, e.g., when
## projecting a record to a subset of its fields.
sub decode_map {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my %hash;

    my $block_count = decode_long($class, @_);
    while ($block_count) {
        my $block_size;
        if ($block_count < 0) {
            $block_count = -$block_count;
            $block_size = decode_long($class, @_);
            ## XXX we can skip with $reader_schema?
        }
        for (1..$block_count) {
            my $key = decode_string($class, @_);
            $hash{$key} = $class->decode(
                writer_schema => $writer_schema->values,
                reader_schema => $reader_schema->values,
                reader        => $reader,
            );
        }
        $block_count = decode_long($class, @_);
    }
    return \%hash;
}

## 1.3.2 A union is encoded by first writing a long value indicating the
## zero-based position within the union of the schema of its value. The value
## is then encoded per the indicated schema within the union.
sub decode_union {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    my $idx = decode_long($class, @_);
    my $union_schema = $writer_schema->schemas->[$idx];
    ## XXX TODO: schema resolution
    return $class->decode(
        reader_schema => $union_schema,
        writer_schema => $union_schema,
        reader => $reader,
    );
}

## 1.3.2 Fixed instances are encoded using the number of bytes declared in the
## schema.
sub decode_fixed {
    my $class = shift;
    my ($writer_schema, $reader_schema, $reader) = @_;
    ## TODO: what if schemas don't match
    $reader->read(my $buf, $writer_schema->size);
    return $buf;
}

sub zigzag {
    my $int = shift;
    if (1 & $int) {
        ## odd values are encoded negative ints
        return -( 1 + ($int >> 1) );
    }
    ## even values are positive natural left shifted one bit
    else {
        return $int >> 1;
    }
}

sub unsigned_varint {
    my $reader = shift;
    my $int = 0;
    my $more;
    my $shift = 0;
    do {
        $reader->read(my $buf, 1);
        my $byte = ord($buf);
        my $value = $byte & 0x7F;
        $int <<= $shift;
        $int |= $value;
        $shift += 7;
        $more = $byte & 0x80;
    } until (! $more);
    return $int;
}

1;
