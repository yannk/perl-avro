package Avro::BinaryEncoder;
use strict;
use warnings;

use Encode();
use Error::Simple;
use Config;

our $complement = ~0x7F;
unless ($Config{use64bitint}) {
    require Math::BigInt;
    $complement = Math::BigInt->new("0x" . ("1" x 57) . ("0" x 7) . "b");
}

sub encode {
    my $class = shift;
    my ($schema, $data, $cb) = @_;

    my $type = $schema->type;

    ## might want to profile and optimize this
    my $meth = "encode_$type";
    $class->$meth($schema, $data, $cb);
    return;
}

sub encode_null {
    $_[3]->(\'');
}

sub encode_boolean {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    $cb->( $data ? \0x1 : \0x0 );
}

sub encode_int {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    my @count = unpack "W*", $data;
    if (scalar @count > 4) {
        throw Avro::BinaryEncoder::Error("int should be 32bits");
    }

    my $enc = unsigned_varint(zigzag($data));
    $cb->(\$enc);
}

sub encode_long {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    my @count = unpack "W*", $data;
    if (scalar @count > 8) {
        throw Avro::BinaryEncoder::Error("int should be 64bits");
    }
    my $enc = unsigned_varint(zigzag($data));
    $cb->(\$enc);
}

sub encode_float {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    my $enc = pack "f<", $data;
    $cb->(\$enc);
}

sub encode_double {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    my $enc = pack "d<", $data;
    $cb->(\$enc);
}

sub encode_bytes {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    encode_long(undef, bytes::length($data), $cb);
    $cb->(\$data);
}

sub encode_string {
    my $class = shift;
    my ($schema, $data, $cb) = @_;
    my $bytes = Encode::encode_utf8($data);
    encode_long(undef, bytes::length($bytes), $cb);
    $cb->(\$bytes);
}

sub zigzag {
    use warnings FATAL => 'numeric';
    if ( $_[0] >= 0 ) {
        return $_[0] << 1;
    }
    return (($_[0] << 1) ^ -1) | 0x1;
}

sub unsigned_varint {
    my @bytes;
    while ($_[0] & $complement ) {          # mask with continuation bit
        push @bytes, ($_[0] & 0x7F) | 0x80; # out and set continuation bit
        $_[0] >>= 7;                        # next please
    }
    push @bytes, $_[0]; # last byte
    return pack "W*", @bytes; ## TODO C
}

package Avro::BinaryEncoder::Error;
use parent 'Error::Simple';

1;
