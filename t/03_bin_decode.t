#!/usr/bin/env perl

use strict;
use warnings;
use Avro::Schema;
use Avro::BinaryEncoder;
use Test::More tests => 12;
use Test::Exception;
use IO::String;

use_ok 'Avro::BinaryDecoder';

## spec examples
{
    my $enc = "\x06\x66\x6f\x6f";
    my $schema = Avro::Schema->parse(q({ "type": "string" }));
    my $reader = IO::String->new($enc);
    my $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is $dec, "foo", "Binary_Encodings.Primitive_Types";

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
    $reader = IO::String->new("\x36\x06\x66\x6f\x6f");
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is_deeply $dec, { a => 27, b => 'foo' },
                    "Binary_Encodings.Complex_Types.Records";

    $reader = IO::String->new("\x04\x06\x36\x00");
    $schema = Avro::Schema->parse(q({"type": "array", "items": "long"}));
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader        => $reader,
    );
    is_deeply $dec, [3, 27], "Binary_Encodings.Complex_Types.Arrays";

    $reader = IO::String->new("\x02");
    $schema = Avro::Schema->parse(q(["string","null"]));
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema,
        reader         => $reader,
    );
    is $dec, undef, "Binary_Encodings.Complex_Types.Unions-null";

    $reader =  IO::String->new("\x00\x02\x61");
    $dec = Avro::BinaryDecoder->decode(
        writer_schema => $schema,
        reader_schema => $schema, 
        reader        => $reader,
    );
    is $dec, "a", "Binary_Encodings.Complex_Types.Unions-a";
}

## schema resolution
{

    my $w_enum = Avro::Schema->parse(<<EOP);
{ "type": "enum", "name": "enum", "symbols": [ "a", "b", "c", "\$", "z" ] }
EOP
    my $r_enum = Avro::Schema->parse(<<EOP);
{ "type": "enum", "name": "enum", "symbols": [ "\$", "b", "c", "d" ] }
EOP
    ok ! !Avro::Schema->match( reader => $r_enum, writer => $w_enum ), "match";
    my $enc;
    for my $data (qw/b c $/) {
        Avro::BinaryEncoder->encode(
            schema  => $w_enum,
            data    => $data,
            emit_cb => sub { $enc = ${ $_[0] } },
        );
        my $dec = Avro::BinaryDecoder->decode(
            writer_schema => $w_enum,
            reader_schema => $r_enum,
            reader => IO::String->new($enc),
        );
        is $dec, $data, "decoded!";
    }

    for my $data (qw/a z/) {
        Avro::BinaryEncoder->encode(
            schema  => $w_enum,
            data    => $data,
            emit_cb => sub { $enc = ${ $_[0] } },
        );
        throws_ok { Avro::BinaryDecoder->decode(
            writer_schema => $w_enum,
            reader_schema => $r_enum,
            reader => IO::String->new($enc),
        )} "Avro::Schema::Error::DataMismatch", "schema problem";
    }
}

done_testing;
