require 'benchmark'

JRUBY = defined?(JRUBY_VERSION)

if JRUBY
  require 'jruby'
  begin
    require "jruby-memcached-1.0-SNAPSHOT.jar"

    com.headius.spymemcached.SpymemcachedLibrary.new.load(JRuby.runtime, false)
  rescue LoadError
    require 'rubygems'
    require 'spymemcached'
  end
  
  memcached = Spymemcached.new(['localhost:11211'])
else
  require 'memcached'

  memcached = Memcached::Rails.new(['localhost:11211'])
  memcached_pipelined = Memcached::Rails.new(['localhost:11211'], :noreply => true)
end

(ARGV[0] || 5).to_i.times {
  Benchmark.bm(30) {|bm|
    bm.report("set") {
      100_000.times { memcached.set('foo', 'bar') }
    }
    bm.report("async_set") {
      100_000.times { memcached.async_set('foo', 'bar') }
    } if JRUBY
    bm.report("quiet_set") {
      100_000.times { memcached.quiet_set('foo', 'bar') }
    } if JRUBY
    bm.report("set pipelined") {
      100_000.times { memcached_pipelined.set('foo', 'bar') }
    } if !JRUBY
    bm.report("get") {
      100_000.times { memcached.get('foo') }
    }
    bm.report("get threaded") {
      (1..4).map {
        Thread.new {
          25_000.times { memcached.get('foo') }
        }
      }.map(&:join)
    }
    bm.report("multiget") {
      25_000.times { memcached.multiget(['foo', 'foo', 'foo', 'foo']) }
    } if JRUBY
    bm.report("async_get") {
      100_000.times { memcached.async_get('foo') }
    } if JRUBY
    bm.report("async_get threaded") {
      (1..4).map {
        Thread.new {
          25_000.times { memcached.async_get('foo') }
        }
      }.map(&:join)
    } if JRUBY
    bm.report("async_multiget") {
      25_000.times { memcached.async_multiget(['foo', 'foo', 'foo', 'foo']) }
    } if JRUBY
  }
}

memcached.shutdown if JRUBY