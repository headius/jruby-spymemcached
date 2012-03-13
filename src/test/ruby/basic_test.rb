require 'test/unit'

$:.unshift File.expand_path(File.dirname(__FILE__) + '/../../../target')
$:.unshift File.expand_path(File.dirname(__FILE__) + "/../../main/ruby")

require 'java'
require 'jruby-spymemcached'
import 'net.spy.memcached.MemcachedClient'
import 'java.net.InetSocketAddress'

class BasicTest < Test::Unit::TestCase
  CONN = MemcachedClient.new([InetSocketAddress.new("localhost", 11211)])
  SPY = Spymemcached.new(["localhost:11211"])

  def setup
    @skip_flush = false
  end

  def teardown
    unless @skip_flush
      native_conn.flush
      spy.clear
    end
  end

  def test_add
    spy.add("foo", "bar")
    assert_equal "bar", Marshal.load(native_conn.get("foo"))
  end

  def test_add_does_nothing_if_value_exists
    native_conn.set("foo", 0, "bar")
    spy.add("foo", "bash")
    assert_equal "bar", native_conn.get("foo")
  end

  def test_incr_with_no_existing_entry_starts_at_one
    spy.incr("one")
    assert_equal 1, native_conn.get("one").to_i
  end

  def test_incr_with_existing_value_adds_one
    spy.set("two", 1)
    spy.incr("two")
    assert_equal 2, native_conn.get("two").to_i
  end

  def test_decr_with_existing_value_subtracts_one
    spy.set("three", 4)
    spy.decr("three")
    assert_equal 3, native_conn.get("three").to_i
  end

  def test_clear_removes_all_keys
    spy.set("four", 4)
    spy.clear
    assert_nil native_conn.get("four")
  end

  def test_delete
    native_conn.set("delete_me", 0, "deleted")
    assert native_conn.get("delete_me")
    spy.delete("delete_me")
    assert_nil native_conn.get("delete_me")
  end

  def test_namespacing_works
    namespaced = Spymemcached.new(["localhost:11211"], :namespace => "jruby")
    namespaced.set("yes", "yes")
    assert_nil spy.get("yes")
    assert_equal "yes", namespaced.get("yes")
    namespaced.shutdown
  end

  def test_stats
    assert spy.stats.keys.length > 0
  end

  # Sadly, this test is here just because Test::Unit::TestCase runs tests
  # in alpha order, and the version of minitest in JRuby is old enough
  # that I don't have any of the suite-level methods available to tear
  # down these instances so that the process will exit.
  def test_z_shutdown_connection
    native_conn.shutdown
    spy.shutdown
    @skip_flush = true
  end

  def spy
    SPY
  end

  def native_conn
    CONN
  end
end

