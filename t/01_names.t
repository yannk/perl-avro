use strict;
use warnings;

use Test::More;
plan tests => 95;
use Test::Exception;
use_ok 'Avro::Schema';

## name validation
{
    my @bad_names = qw/0 01 0a $ % $s . - -1 (x) #s # π
                       @ !q ^f [ ( { } ) ] ~ ` ?a :a ;a 
                       a- a^ a% a[ .. ... .a .a. a./;

    my @bad_namespaces = @bad_names;
    for my $name (@bad_names) {
        throws_ok { Avro::Schema::Record->new(
            struct => {
                name => $name,
                fields => [ { name => 'a', type => 'long' } ],
            },
        ) } "Avro::Schema::Error::Name", "bad name: $name";
    }

    for my $ns (@bad_namespaces) {
        throws_ok { Avro::Schema::Record->new(
            struct => {
                name => 'name',
                namespace => $ns,
                fields => [ { name => 'a', type => 'long' } ],
            },
        ) } "Avro::Schema::Error::Name", "bad ns: $ns";
    }
}

## name + namespace (bullet 1 of spec)
{
    my $r = Avro::Schema::Record->new(
        struct => {
            name => 'saucisson',
            namespace => 'dry',
            fields => [ { name => 'a', type => 'long' } ],
        },
    );
    is $r->fullname, 'dry.saucisson', "correct fullname";
    is $r->namespace, 'dry', "ns is dry";
}

## fullname (bullet 2 of spec)
{
    my $r = Avro::Schema::Record->new(
        struct => {
            name => 'dry.saucisson',
            fields => [ { name => 'a', type => 'long' } ],
        },
    );
    is $r->fullname, 'dry.saucisson', "correct fullname";
    is $r->namespace, 'dry', "ns is dry";

    $r = Avro::Schema::Record->new(
        struct => {
            name => 'dry.saucisson',
            namespace => 'archiduchesse.chaussette', ## ignored
            fields => [ { name => 'a', type => 'long' } ],
        },
    );
    is $r->fullname, 'dry.saucisson', "correct fullname";
    is $r->namespace, 'dry', "ns is dry";
}

## name only (bullet 3 of spec)
{
    my $r = Avro::Schema::Record->new(
        struct => {
            name => 'container',
            namespace => 'dry',
            fields => [ {
                name => 'a', type => {
                    type => 'record', name => 'saucisson', fields => [
                        { name => 'aa', type => 'long' },
                    ],
                }
            } ],
        },
    );
    is $r->fullname, 'dry.container', "correct fullname";
    is $r->namespace, 'dry', "ns is dry";
    my $subr = $r->fields->[0]{type};
    is $subr->fullname, 'dry.saucisson', 'dry.saucisson';
    is $subr->namespace, 'dry', "sub ns is dry";

    $r = Avro::Schema::Record->new(
        struct => {
            name => 'dry.container',
            fields => [ {
                name => 'a', type => {
                    type => 'record', name => 'saucisson', fields => [
                        { name => 'aa', type => 'long' },
                    ],
                }
            } ],
        },
    );
    is $r->fullname, 'dry.container', "correct fullname";
    is $r->namespace, 'dry', "ns is dry";
    $subr = $r->fields->[0]{type};
    is $subr->fullname, 'dry.saucisson', 'dry.saucisson';
    is $subr->namespace, 'dry', "sub ns is dry";

    $r = Avro::Schema::Record->new(
        struct => {
            name => 'dry.container',
            fields => [ {
                name => 'a', type => {
                    type => 'record', name => 'duchesse.saucisson', fields => [
                        { name => 'aa', type => 'long' },
                    ],
                }
            } ],
        },
    );
    is $r->fullname, 'dry.container', "correct fullname";
    is $r->namespace, 'dry', "ns is dry";
    $subr = $r->fields->[0]{type};
    is $subr->fullname, 'duchesse.saucisson', 'duchesse.saucisson';
    is $subr->namespace, 'duchesse', "sub ns is duchesse";

    $r = Avro::Schema::Record->new(
        struct => {
            name => 'dry.container',
            fields => [ {
                name => 'a', type => {
                    type => 'record',
                    namespace => 'duc',
                    name => 'saucisson',
                    fields => [
                        { name => 'aa', type => 'long' },
                    ],
                }
            } ],
        },
    );
    is $r->fullname, 'dry.container', "correct fullname";
    is $r->namespace, 'dry', "ns is dry";
    $subr = $r->fields->[0]{type};
    is $subr->fullname, 'duc.saucisson', 'duc.saucisson';
    is $subr->namespace, 'duc', "sub ns is duc";
}

done_testing;
