#!/usr/bin/env perl

use strict;
use warnings;
use Avro::DataFile;
use Avro::BinaryEncoder;
use Avro::BinaryDecoder;
use Avro::Schema;
use File::Temp;
use Test::Exception;
use Test::More tests => 12;

use_ok 'Avro::DataFileReader';
use_ok 'Avro::DataFileWriter';

my $tmpfh = File::Temp->new(UNLINK => 1);

my $schema = Avro::Schema->parse(<<EOP);
{ "type": "map", "values": { "type": "array", "items": "string" }}
EOP

my $write_file = Avro::DataFileWriter->new(
    fh            => $tmpfh,
    writer_schema => $schema,
    metadata      => {
        some => 'metadata',
    },
);

my $data = {
    a => [ "2.2", "4.4" ],
    b => [ "2.4", "2", "-4", "4", "5" ],
    c => [ "0" ],
};

$write_file->print($data);
$write_file->flush;

## rewind
seek $tmpfh, 0, 0;
my $uncompressed_size = -s $tmpfh;

my $read_file = Avro::DataFileReader->new(
    fh            => $tmpfh,
    reader_schema => $schema,
);
is $read_file->metadata->{'avro.codec'}, 'null', 'avro.codec';
is $read_file->metadata->{'some'}, 'metadata', 'custom meta';

my @all = $read_file->all;
is scalar @all, 1, "one object back";
is_deeply $all[0], $data, "Our data is intact!";


## codec tests
{
    throws_ok {
        Avro::DataFileWriter->new(
            fh            => File::Temp->new,
            writer_schema => $schema,
            codec         => 'unknown',
        );
    } "Avro::DataFile::Error::InvalidCodec", "invalid codec";

    ## rewind
    seek $tmpfh, 0, 0;
    local $Avro::DataFile::ValidCodec{null} = 0;
    $read_file = Avro::DataFileReader->new(
        fh            => $tmpfh,
        reader_schema => $schema,
    );

    throws_ok {
        $read_file->all;
    } "Avro::DataFile::Error::UnsupportedCodec", "I've removed 'null' :)";

    ## deflate!
    my $zfh = File::Temp->new(UNLINK => 0);
    my $write_file = Avro::DataFileWriter->new(
        fh            => $zfh,
        writer_schema => $schema,
        codec         => 'deflate',
        metadata      => {
            some => 'metadata',
        },
    );
    $write_file->print($data);
    $write_file->flush;

    ## rewind
    seek $zfh, 0, 0;

    my $read_file = Avro::DataFileReader->new(
        fh            => $zfh,
        reader_schema => $schema,
    );
    is $read_file->metadata->{'avro.codec'}, 'deflate', 'avro.codec';
    is $read_file->metadata->{'some'}, 'metadata', 'custom meta';

    my @all = $read_file->all;
    is scalar @all, 1, "one object back";
    is_deeply $all[0], $data, "Our data is intact!";
}

done_testing;
