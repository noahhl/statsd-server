# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)
require 'statsd_server/version'
Gem::Specification.new do |s|
  s.name        = "statsd-server"
  s.version     = StatsdServer::VERSION 
  s.platform    = Gem::Platform::RUBY
  s.authors     = ['Noah Lorang']
  s.email       = ['noah@datarobots.com']
  s.homepage    = "http://github.com/noahhl/statsd-server"
  s.summary     = "Ruby version of statsd that stores data in redis and on disk"
  s.description = "A network daemon for aggregating statistics (counters and timers), rolling them up, then sending them to redis and a simple disk store. Inspired by http://github.com/quasor/statsd by Andrew Coldham and Ben VandenBos"
  
  s.required_rubygems_version = ">= 1.3.6"

  s.add_dependency "eventmachine",  ">= 1.0.0.beta.4"
  s.add_dependency "erubis",        ">= 2.6.6"
  s.add_dependency "redis",         ">= 2.0.0"

  s.files        = `git ls-files`.split("\n")
  s.executables  = `git ls-files`.split("\n").map{|f| f =~ /^bin\/(.*)/ ? $1 : nil}.compact
  s.require_path = 'lib'
end

