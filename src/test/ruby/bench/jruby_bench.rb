require 'benchmark'

JRUBY = defined?(JRUBY_VERSION)

if JRUBY
  require 'jruby'

  com.headius.memcached.MemcachedLibrary.new.load(JRuby.runtime, false)

  memcached = JRuby::Memcached.new(['localhost:11211'])
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
    bm.report("set_lazy") {
      100_000.times { memcached.set_lazy('foo', 'bar') }
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
    bm.report("get_multi") {
      25_000.times { memcached.get_multi(['foo', 'foo', 'foo', 'foo']) }
    } if JRUBY
    bm.report("get_lazy") {
      100_000.times { memcached.get_lazy('foo') }
    } if JRUBY
    bm.report("get_lazy threaded") {
      (1..4).map {
        Thread.new {
          25_000.times { memcached.get_lazy('foo') }
        }
      }.map(&:join)
    } if JRUBY
    bm.report("get_lazy_multi") {
      25_000.times { memcached.get_lazy_multi(['foo', 'foo', 'foo', 'foo']) }
    } if JRUBY
  }
}
