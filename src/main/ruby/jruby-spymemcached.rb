module JRubySpymemcached
  VERSION = '0.0.1'
end

require "jruby-memcached-#{JRubySpymemcached::VERSION}.jar"
require 'jruby'

com.headius.spymemcached.SpymemcachedLibrary.new.load(JRuby.runtime, false)