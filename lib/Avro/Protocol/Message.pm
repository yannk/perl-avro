package Avro::Protocol::Message;

use strict;
use warnings;

use Avro::Schema;
use Avro::Protocol;
use Error;

use Object::Tiny qw{
    doc
    request
    response
    errors
};

sub new {
    my $class = shift;
    my $struct = shift;
    my $types = shift;

    my $resp_struct = $struct->{response}
        or throw Avro::Protocol::Error::Parse("response is missing");

    my $req_struct = $struct->{request}
        or throw Avro::Protocol::Error::Parse("request is missing");

    my $request = [
        map { Avro::Schema::Field->new($_, $types) } @$req_struct
    ];

    my $err_struct = $struct->{errors};

    my $response = Avro::Schema->parse_struct($resp_struct, $types);
    my $errors   = Avro::Schema->parse_struct($err_struct, $types)
        if $err_struct;

    return $class->SUPER::new(
        doc      => $struct->{doc},
        request  => $request,
        response => $response,
        errors   => $errors,
    );

}

1;
