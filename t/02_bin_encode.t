#!/usr/bin/env perl

use strict;
use warnings;
use Test::More tests => 17;
use Test::Exception;
use Math::BigInt;

use_ok 'Avro::BinaryEncoder';

sub primitive_ok {
    my ($primitive_type, $primitive_val, $expected_enc) = @_;

    my $data;
    my $meth = "encode_$primitive_type";
    Avro::BinaryEncoder->$meth(
        undef, $primitive_val, sub { $data = ${$_[0]} }
    );
    is $data, $expected_enc, "primitive $primitive_type encoded correctly";
    return $data;
}
primitive_ok null    =>    undef, '';
primitive_ok null    => 'whatev', '';

## - high-bit of each byte should be set execpt for last one
## - rest of bits are:
## - little endian
## - zigzag coded
primitive_ok long    =>        0, pack("C*", 0);
primitive_ok long    =>        1, pack("C*", 0x2);
primitive_ok long    =>       -1, pack("C*", 0x1);
primitive_ok int     =>       -1, pack("C*", 0x1);
primitive_ok int     =>      -20, pack("C*", 0b0010_0111);
primitive_ok int     =>       20, pack("C*", 0b0010_1000);
primitive_ok int     =>       63, pack("C*", 0b0111_1110);
primitive_ok int     =>       64, pack("C*", 0b1000_0000, 0b0000_0001);
my $p =
primitive_ok int     =>      -65, pack("C*", 0b1000_0001, 0b0000_0001);

## BigInt values still work
primitive_ok int     => Math::BigInt->new(-65), $p;

throws_ok {
    primitive_ok int => 1<<32, undef;
} "Avro::BinaryEncoder::Error", "33 bits";

throws_ok {
    primitive_ok int => Math::BigInt->new(1)->blsft(63), undef;
} "Avro::BinaryEncoder::Error", "65 bits";

for (qw(long int)) {
    dies_ok {
        primitive_ok $_ =>  "x", undef;
    } "numeric values only";
}


done_testing;
