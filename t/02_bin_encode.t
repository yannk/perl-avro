#!/usr/bin/env perl

use strict;
use warnings;
use Avro::Schema;
use Config;
use Test::More tests => 30;
use Test::Exception;
use Math::BigInt;

use_ok 'Avro::BinaryEncoder';

sub hexdump {
    return unpack 'H*', shift;
}

sub primitive_ok {
    my ($primitive_type, $primitive_val, $expected_enc) = @_;

    my $data;
    my $meth = "encode_$primitive_type";
    Avro::BinaryEncoder->$meth(
        undef, $primitive_val, sub { $data = ${$_[0]} }
    );
    #is $data, $expected_enc, "primitive $primitive_type encoded correctly";
    if (defined $expected_enc) {
        is hexdump($data), hexdump($expected_enc),
            "primitive $primitive_type encoded correctly";
    }
    return $data;
}

## some primitive testing
{
    primitive_ok null    =>    undef, '';
    primitive_ok null    => 'whatev', '';

    primitive_ok boolean => 0, "\x0";
    primitive_ok boolean => 1, "\x1";
    throws_ok {
        primitive_ok boolean => 31415, undef;
    } 'Avro::BinaryEncoder::Error', 'undef as boolean';

    ## - high-bit of each byte should be set except for last one
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
    primitive_ok int     =>       65, pack("C*", 0b1000_0010, 0b0000_0001);
    primitive_ok int     =>       99, "\xc6\x01";

    ## BigInt values still work
    primitive_ok int     => Math::BigInt->new(-65), $p;

    throws_ok {
        my $toobig;
        if ($Config{use64bitint}) {
            $toobig = 1<<32;
        }
        else {
            require Math::BigInt;
            $toobig = Math::BigInt->new(1)->blsft(32);
        }
        primitive_ok int => $toobig, undef;
    } "Avro::BinaryEncoder::Error", "33 bits";

    throws_ok {
        primitive_ok int => Math::BigInt->new(1)->blsft(63), undef;
    } "Avro::BinaryEncoder::Error", "65 bits";

    for (qw(long int)) {
        dies_ok {
            primitive_ok $_ =>  "x", undef;
        } "numeric values only";
    }
}

## spec examples
{
    my $enc = '';
    my $schema = Avro::Schema->parse(q({ "type": "string" }));
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => "foo",
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is hexdump($enc), '06666f6f', "Binary_Encodings.Primitive_Types";

    $schema = Avro::Schema->parse(<<EOJ);
          {
          "type": "record",
          "name": "test",
          "fields" : [
          {"name": "a", "type": "long"},
          {"name": "b", "type": "string"}
          ]
          }
EOJ
    $enc = '';
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => { a => 27, b => 'foo' },
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is hexdump($enc), '3606666f6f', 'Binary_Encodings.Complex_Types.Records';

    $enc = '';
    $schema = Avro::Schema->parse(q({"type": "array", "items": "long"}));
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => [3, 27],
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is hexdump($enc), '04063600', 'Binary_Encodings.Complex_Types.Arrays';  

    $enc = '';
    $schema = Avro::Schema->parse(q(["string","null"]));
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => undef,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is hexdump($enc), '02', 'Binary_Encodings.Complex_Types.Unions-null';

    $enc = '';
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => "a",
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is hexdump($enc), '000261', 'Binary_Encodings.Complex_Types.Unions-a';
}

# boolean unions
{
    my $enc = '';
    my $schema = Avro::Schema->parse(q(["boolean", "null"]));
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => undef,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is hexdump($enc), '02', 'Binary_Encodings.Complex_Types.Unions-boolean-undef';

    $enc = '';
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => 0,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is hexdump($enc), '0000', 'Binary_Encodings.Complex_Types.Unions-boolean-false';

    $enc = '';
    Avro::BinaryEncoder->encode(
        schema => $schema,
        data => 1,
        emit_cb => sub { $enc .= ${ $_[0] } },
    );
    is hexdump($enc), '0001', 'Binary_Encodings.Complex_Types.Unions-boolean-true';
}

done_testing;
