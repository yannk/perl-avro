use strict;
use warnings;

use Test::More;
plan tests => 22;
use Test::Exception;
use_ok 'Avro::Schema';

dies_ok { Avro::Schema->new } "Should use parse() or instanciate the subclass";

throws_ok { Avro::Schema->parse(q()) } "Avro::Schema::Error::ParseError";
throws_ok { Avro::Schema->parse(q(test)) } "Avro::Schema::Error::ParseError";

my $s = Avro::Schema->parse(q("string"));
isa_ok $s, 'Avro::Schema::Base';
isa_ok $s, 'Avro::Schema::Primitive',
is $s->type, "string", "type is string";

my $s2 = Avro::Schema->parse(q({"type": "string"}));
isa_ok $s2, 'Avro::Schema::Primitive';
is $s2->type, "string", "type is string";
is $s, $s2, "string Schematas are singletons";

my $s3 = Avro::Schema::Record->new(
    struct => {
        name => 'saucisson',
        fields => [
            { name => 'a', type => 'long'   },
            { name => 'b', type => 'string' },
        ],
    },
);

isa_ok $s3, 'Avro::Schema::Record';
is $s3->fullname, 'saucisson', "correct name";
is $s3->fields->[0]{name}, 'a', 'a';
is $s3->fields->[0]{type}, Avro::Schema::Primitive->new(type => 'long'), 'long';
is $s3->fields->[1]{name}, 'b', 'b';
is $s3->fields->[1]{type}, Avro::Schema::Primitive->new(type => 'string'), 'str';

## self-refence
$s3 = Avro::Schema::Record->new(
    struct => {
        name => 'saucisson',
        fields => [
            { name => 'a', type => 'long'   },
            { name => 'b', type => 'saucisson' },
        ],
    },
);
isa_ok $s3, 'Avro::Schema::Record';
is $s3->fullname, 'saucisson', "correct name";
is $s3->fields->[0]{name}, 'a', 'a';
is $s3->fields->[0]{type}, Avro::Schema::Primitive->new(type => 'long'), 'long';
is $s3->fields->[1]{name}, 'b', 'b';
is $s3->fields->[1]{type}, $s3, 'self!';

done_testing;

__DATA__
{ "string" }
-> Avro::Schema::Primitive(string)

{ "type": "string" }
-> Avro::Schema::Primitive(string)

{ "type": "enum",
  "name": "Suit",
  "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
}
-> Avro::Schema::Enum(name => suit, symbols = [])

{
    "type": "record", 
    "name": "test",
    "fields" : [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"}
    ]
}
-> Avro::Schema::Record(name => test, fields => [
        { name => 'a', type => Avro::Schema::Primitive(long) }
        { name => 'b', type => Avro::Schema::Primitive(string) }
    ]
)

{
  "type": "record", 
  "name": "LongList",
  "fields" : [
    {"name": "value", "type": "long"},             // each element has a long
    {"name": "next", "type": ["LongList", "null"]} // optional next element
  ]
}

-> Avro::Schema::Record(name => LongList, fields => [
        { name => 'value', type => Avro::Schema::Primitive(long) }
        { name => 'next', type => Avro::Schema::Union(fields => [
            Avro::Schema::Named('LongList'),
            Avro::Schema::Primitive(null)
            ])
        }
    ]
)
