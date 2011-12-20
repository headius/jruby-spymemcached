# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{jruby-spymemcached}
  s.version = "0.0.1"
  s.authors = ["Charles Oliver Nutter"]
  s.date = Time.now.strftime('%Y-%m-%d')
  s.description = %q{A JRuby extension wrapping the spymemcached library}
  s.email = ["headius@headius.com"]
  s.extra_rdoc_files = Dir['*.txt']
  s.files =
    Dir['target/*.jar'] +
    Dir['src/main/ruby/**'] +
    Dir['src/test/ruby/**'] +
    Dir['{*.md,*.gemspec,Rakefile}']
  s.homepage = %q{http://github.com/headius/jruby-spymemcached}
  s.require_paths = ['target', 'src/main/ruby']
  s.summary = %q{A JRuby extension wrapping the spymemcached library}
end
