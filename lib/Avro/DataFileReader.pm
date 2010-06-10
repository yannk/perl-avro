package Avro::DataFileReader;
use strict;
use warnings;

use Object::Tiny qw{
    fh
    reader_schema
    sync_marker
    block_max_size
};

use constant MARKER_SIZE => 16;

# TODO: refuse to read a block more than block_max_size, instead
# do partial reads

use Avro::DataFile;
use Avro::BinaryDecoder;
use Avro::Schema;
use Carp;
use IO::String;

sub new {
    my $class = shift;
    my $datafile = $class->SUPER::new(@_);

    my $schema = $datafile->{reader_schema};
    croak "schema is invalid"
        if $schema && ! eval { $schema->isa("Avro::Schema") };

    return $datafile;
}

sub codec {
    my $datafile = shift;
    return $datafile->metadata->{'avro.codec'};
}

sub writer_schema {
    my $datafile = shift;
    unless (exists $datafile->{_writer_schema}) {
        my $json_schema = $datafile->metadata->{'avro.schema'};
        $datafile->{_writer_schema} = Avro::Schema->parse($json_schema);
    }
    return $datafile->{_writer_schema};
}

sub metadata {
    my $datafile = shift;
    unless (exists $datafile->{_metadata}) {
        my $header = $datafile->header;
        $datafile->{_metadata} = $header->{meta} || {};
    }
    return $datafile->{_metadata};
}

sub header {
    my $datafile = shift;
    unless (exists $datafile->{_header}) {
        $datafile->{_header} = $datafile->read_file_header;
    }

    return $datafile->{_header};
}

sub read_file_header {
    my $datafile = shift;

    my $data = Avro::BinaryDecoder->decode(
        reader_schema => $Avro::DataFile::HEADER_SCHEMA,
        writer_schema => $Avro::DataFile::HEADER_SCHEMA,
        reader        => $datafile->{fh},
    );
    croak "Magic '$data->{magic}' doesn't match"
        unless $data->{magic} eq Avro::DataFile->AVRO_MAGIC;

    $datafile->{sync_marker} = $data->{sync}
        or croak "sync marker appears invalid";

    my $codec = $data->{meta}{'avro.codec'} || "";

    throw Avro::DataFile::Error::UnsupportedCodec($codec)
        unless Avro::DataFile->is_codec_valid($codec);

    return $data;
}

sub all {
    my $datafile = shift;

    my @objs;
    my @block_objs;
    do {
        @block_objs = $datafile->read_to_block_end;
        push @objs, @block_objs

    } until !@block_objs;

    return @objs
}

sub next {
    my $datafile = shift;
    my $count    = shift;

    my @objs;
    my $block_count = $datafile->{object_count};
    if ($block_count <= $count) {
        push @objs, $datafile->read_to_block_end;
        croak "Didn't read as many objects than expected"
            unless scalar @objs == $block_count;

        $datafile->next($count - $block_count);
    }
    else {
        ## could probably be optimized
        my $fh            = $datafile->{fh};
        my $writer_schema = $datafile->writer_schema;
        my $reader_schema = $datafile->reader_schema;
        while ($count--) {
            push @objs, Avro::BinaryDecoder->decode(
                $writer_schema,
                $reader_schema,
                $fh,
            );
            $datafile->{object_count}--;
        }
    }
}

sub skip {
    my $datafile = shift;
    my $count    = shift;

    my $block_count = $datafile->{object_count};
    if ($block_count <= $count) {
        $datafile->skip_to_block_end
            or croak "Cannot skip to end of block!";
        $datafile->skip($count - $block_count);
    }
    else {
        my $writer_schema = $datafile->writer_schema;
        ## could probably be optimized
        while ($count--) {
            Avro::BinaryDecoder->skip($writer_schema, $datafile->{fh});
            $datafile->{object_count}--;
        }
    }
}

sub read_block_header {
    my $datafile = shift;
    my $fh = $datafile->{fh};

    $datafile->header
        unless $datafile->{_header};

    $datafile->{object_count} = Avro::BinaryDecoder->decode_long(
        undef, undef, $fh,
    );
    $datafile->{block_size} = Avro::BinaryDecoder->decode_long(
        undef, undef, $fh,
    );
    $datafile->{block_start} = tell $fh;
    return 1;
}

sub remaining_size {
    my $datafile = shift;

    return   $datafile->{block_size}
           + $datafile->{block_start}
           - tell $datafile->{fh};
}

sub skip_to_block_end {
    my $datafile = shift;

    return if $datafile->{fh}->eof;

    $datafile->read_block_header
        if $datafile->eob;

    seek $datafile->{fh}, $datafile->remaining_size + MARKER_SIZE, 0;
    return 1;
}

sub read_to_block_end {
    my $datafile = shift;

    my $fh = $datafile->{fh};
    return () if $fh->eof;

    $datafile->read_block_header
        if $datafile->eob;

    my $nread = read $fh, my $buffer, $datafile->remaining_size + MARKER_SIZE
        or croak "Error reading from file: $!";

    if ( $nread <  $datafile->remaining_size + MARKER_SIZE ) {
        warn "read less than expected: $nread";
    }

    my $block_buffer = IO::String->new($buffer);

    my $writer_schema = $datafile->writer_schema;
    my $reader_schema = $datafile->reader_schema || $writer_schema;

    my @objs;
    while ($datafile->{object_count}--) {
        push @objs, Avro::BinaryDecoder->decode(
            writer_schema => $writer_schema,
            reader_schema => $reader_schema,
            reader        => $block_buffer,
        );
    }

    ## some validation checks
    my $tail;
    $nread = $block_buffer->read($tail, MARKER_SIZE);
    if (MARKER_SIZE != $nread ) {
        croak "Oops synchronization issue (size=$nread)";
    }
    unless ($tail eq $datafile->sync_marker) {
        croak "Oops synchronization issue (marker mismatch)";
    }

    return @objs;
}

## end of block
sub eob {
    my $datafile = shift;
    my $pos = tell $datafile->{fh};
    return 1 unless $datafile->{block_start};
    return 1 if $pos >= $datafile->{block_start} + $datafile->{block_size};
    return 0;
}

package Avro::DataFile::Error::UnsupportedCodec;
use parent 'Error::Simple';

1;
