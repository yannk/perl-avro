use Config;
use inc::Module::Install;
license 'apache';
all_from 'lib/Avro.pm';
#readme_from 'lib/Avro.pm';
build_requires 'Test::More', 0.88;
test_requires 'Math::BigInt';
test_requires 'Test::Exception';
requires 'JSON::XS';
requires 'Try::Tiny';
requires 'parent';
requires 'Regexp::Common';
requires 'Encode';
requires 'IO::String';
requires 'Object::Tiny';
requires 'Compress::Zlib';
unless ($Config{use64bitint}) {
    requires 'Math::BigInt';
}
#auto_set_repository();
WriteAll;
