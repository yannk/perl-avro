#!/usr/bin/env perl

use strict;
use warnings;
use Avro::Schema;
use Avro::BinaryEncoder;
use Avro::BinaryDecoder;
use File::Temp;
use Test::Exception;
use Test::More tests => 6;

use_ok 'Avro::DataFileReader';
use_ok 'Avro::DataFileWriter';

my $tmpfh = File::Temp->new(UNLINK => 1);

my $schema = Avro::Schema->parse(<<EOP);
{ "type": "map", "values": { "type": "array", "items": "string" } }
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

=cut
use JSON::XS;
use IO::Compress::Deflate qw(deflate $DeflateError) ;
    $z .= encode_json($data);
my $output;
my $status = deflate \$z => \$output
          or die "deflate failed: $DeflateError\n";
warn (length $output);
=cut

$write_file->print($data);
$write_file->flush;

## rewind
seek $tmpfh, 0, 0;

my $read_file = Avro::DataFileReader->new(
    fh            => $tmpfh,
    reader_schema => $schema,
);
is $read_file->metadata->{'avro.codec'}, 'null', 'avro.codec';
is $read_file->metadata->{'some'}, 'metadata', 'custom meta';

my @all = $read_file->all;
is scalar @all, 1, "one object back";
is_deeply $all[0], $data, "Our data is intact!";

done_testing;
