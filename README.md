This is basically just a thin JRuby extension wrapping the
spymemcached client.

Building
--------

I used Maven because I'm lazy.

```
mvn package
gem build src/main/ruby/jruby-spymemcached.gemspec
```

Usage
-----

```
require 'rubygems'
require 'memcached'

memcached = Spymemcached.new(['localhost:11211'])

memcached.set('foo', 'bar') # synchronous set
future = memcached.async_set('foo', 'bar') # async set
#...
future.get # or not, if you don't care about result

memcached.get('foo')
10.times {
  Thread.new {
    25_000.times { memcached.get('foo') } # concurrency-friendly
  }
}
memcached.multiget(['foo', 'foo', 'foo', 'foo']) # get multi

future = memcached.async_get('foo') # async get, returns a future
#...
future.get

future = memcached.async_multiget(['foo', 'foo', 'foo', 'foo']) # ditto
#...
future.get

memcached.shutdown if JRUBY
```

TODO
----

* API-compatible wrappers for the popular Ruby memcached libs.
* More spymemcached feature exposure
