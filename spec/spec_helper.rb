TEST_SUITE_DEFAULTS = {
  connect_timeout: 5,
  read_timeout: 5,
  write_timeout: 5
}

require 'excon'
require 'excon/test/server'
require 'json'
require 'socket'

# Each test process owns a different loopback endpoint.
EXCON_TEST_PORT = TCPServer.open('127.0.0.1', 0) { |server| server.addr[1] }
ENV['EXCON_TEST_PORT'] = EXCON_TEST_PORT.to_s

# See http://rubydoc.info/gems/rspec-core/RSpec/Core/Configuration
RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

 config.mock_with :rspec do |mocks|
   mocks.verify_partial_doubles = true
  end

  if config.files_to_run.one?
    config.default_formatter = 'doc'
  end

  Excon.set_raise_on_warnings!(true)
end

# Load helpers
Dir["./spec/helpers/**/*.rb"].sort.each { |f| require f}

# Load shared examples and contexts
Dir["./spec/support/**/*.rb"].sort.each { |f| require f}
