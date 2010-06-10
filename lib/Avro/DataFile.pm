package Avro::DataFile;
use strict;
use warnings;

use constant AVRO_MAGIC => "Obj\x01";

use Avro::Schema;

our $HEADER_SCHEMA = Avro::Schema->parse(<<EOH);
{"type": "record", "name": "org.apache.avro.file.Header",
  "fields" : [
    {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
    {"name": "meta", "type": {"type": "map", "values": "bytes"}},
    {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}
  ]
}
EOH

our %ValidCodec = (
    null    => 1,
    deflate => 1,
);

sub is_codec_valid {
    my $datafile = shift;
    my $codec = shift || '';
    return $ValidCodec{$codec};
}

+1;
