This is basically just a thin JRuby extension wrapping the
spymemcached client.

Usage
-----

require 'rubygems'
require 'jruby-memcached'

memcached = Spymemcached.new(['localhost:11211'])

memcached.set('foo', 'bar') # synchronous set
memcached.async_set('foo', 'bar') # async set

memcached.get('foo')
10.times {
  Thread.new {
    25_000.times { memcached.get('foo') } # concurrency-friendly
  }
}
memcached.multiget(['foo', 'foo', 'foo', 'foo']) # get multi

memcached.async_get('foo') # async get, returns a future
memcached.async_multiget(['foo', 'foo', 'foo', 'foo']) # ditto

memcached.shutdown if JRUBY