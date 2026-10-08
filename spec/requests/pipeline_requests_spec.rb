# frozen_string_literal: true

require 'spec_helper'
require 'support/scripted_pipeline_socket'

describe Excon::Connection do
  let(:url) { 'http://pipeline.example.com' }

  def http_response(body, headers = {})
    lines = ["HTTP/1.1 200 OK"]
    lines << "Content-Length: #{body.bytesize}" unless headers.key?('Content-Length')
    headers.each { |key, value| lines << "#{key}: #{value}" }
    lines.join("\r\n") + "\r\n\r\n" + body
  end

  # Build a connection whose socket cache always returns the supplied socket
  # double (or invokes the supplied proc), bypassing real TCP connect.
  def connection_with_socket(socket_or_proc, options = {})
    connection = Excon.new(url, options)
    factory = socket_or_proc
    connection.singleton_class.send(:define_method, :socket) do |*args|
      datum = args.first
      return sockets[@socket_key] if sockets[@socket_key]

      @scripted_raw_socket ||= factory.is_a?(Proc) ? factory.call : factory
      socket = @scripted_raw_socket
      socket.data = datum if socket.respond_to?(:data=)
      sockets[@socket_key] = socket
      socket
    end
    connection
  end

  def result_statuses(results)
    results.map { |result| result[:status] }
  end

  describe '#pipeline_requests' do
    it 'returns an empty Array for empty input without touching a socket' do
      connection = Excon.new(url, :persistent => true)
      expect(connection.pipeline_requests([])).to eq([])
    end

    context 'when every request succeeds' do
      let(:socket) do
        Excon::ScriptedPipelineSocket.new(
          :responses => [http_response('one'), http_response('two'), http_response('three')]
        )
      end
      let(:connection) { connection_with_socket(socket, :persistent => true) }

      it 'returns a completed result for every input position in order' do
        results = connection.pipeline_requests([
          { :path => '/1' }, { :path => '/2' }, { :path => '/3' }
        ])

        expect(result_statuses(results)).to eq(%i[completed completed completed])
        expect(results.map { |result| result[:response].body }).to eq(%w[one two three])
        expect(results.map { |result| result[:error] }).to eq([nil, nil, nil])
      end

      it 'keeps the underlying connection reusable' do
        connection.pipeline_requests([{ :path => '/1' }, { :path => '/2' }])
        expect(socket.closed).to be(false)
      end

      it 'resets the connection when the final response asks to close' do
        closing_socket = Excon::ScriptedPipelineSocket.new(
          :responses => [http_response('one'), http_response('two', 'Connection' => 'close')]
        )
        conn = connection_with_socket(closing_socket, :persistent => true)
        conn.pipeline_requests([{ :path => '/1' }, { :path => '/2' }])
        expect(closing_socket.closed).to be(true)
      end
    end

    context 'when the connection cannot be established' do
      it 'marks every item not_sent and keeps the connect error' do
        connection = connection_with_socket(-> { raise Errno::ECONNREFUSED }, :persistent => true)
        results = connection.pipeline_requests([
          { :path => '/1' }, { :path => '/2' }, { :path => '/3' }
        ])

        expect(result_statuses(results)).to eq(%i[not_sent not_sent not_sent])
        expect(results[0][:error]).to be_a(Errno::ECONNREFUSED)
        expect(results[1][:error]).to be_a(Errno::ECONNREFUSED)
        expect(results[2][:error]).to be_a(Errno::ECONNREFUSED)
      end
    end

    context 'when writing fails without accepting any byte' do
      let(:socket) do
        Excon::ScriptedPipelineSocket.new(
          :responses => [],
          :fail_after_written => 0,
          :fail_accept_bytes => 0
        )
      end
      let(:connection) { connection_with_socket(socket, :persistent => true) }

      it 'reports the zero-byte item as not_sent' do
        results = connection.pipeline_requests([
          { :path => '/one' }, { :path => '/two' }
        ])

        expect(result_statuses(results)).to eq(%i[not_sent not_sent])
        expect(socket.bytes_written).to eq(0)
        expect(socket.closed).to be(true)
      end
    end

    context 'when writing fails halfway through the first request headers' do
      let(:socket) do
        Excon::ScriptedPipelineSocket.new(
          :responses => [],
          :fail_after_written => 7,
          :fail_accept_bytes => 7
        )
      end
      let(:connection) { connection_with_socket(socket, :persistent => true) }

      it 'marks the partially written item sent_outcome_unknown and accepts only the prefix' do
        results = connection.pipeline_requests([
          { :path => '/one' }, { :path => '/two' }, { :path => '/three' }
        ])

        expect(result_statuses(results)).to eq(%i[sent_outcome_unknown not_sent not_sent])
        expect(results[0][:error]).to be_a(Excon::Error::Socket)
        expect(results[1][:error]).to be_nil
        expect(results[2][:error]).to be_nil
        expect(socket.bytes_written).to eq(7)
        expect(socket.closed).to be(true)
      end
    end

    context 'when writing fails after some bytes of the first request are on the wire' do
      let(:socket) do
        Excon::ScriptedPipelineSocket.new(
          :responses => [],
          :fail_after_written => 8
        )
      end
      let(:connection) { connection_with_socket(socket, :persistent => true) }

      it 'classifies the first item as sent_outcome_unknown and the rest not_sent' do
        results = connection.pipeline_requests([
          { :path => '/one' }, { :path => '/two' }, { :path => '/three' }
        ])

        expect(result_statuses(results)).to eq(%i[sent_outcome_unknown not_sent not_sent])
        expect(results[0][:error]).to be_a(Excon::Error::Socket)
        expect(results[1][:error]).to be_nil
        expect(results[2][:error]).to be_nil
        expect(socket.closed).to be(true)
      end
    end

    context 'when writing fails after a complete request body has been sent' do
      let(:socket) do
        Excon::ScriptedPipelineSocket.new(
          :responses => [http_response('one')],
          # Write 1: GET /one. For the POST, write 2 carries its headers plus
          # the first 4 body bytes, and write 3 delivers the remaining bytes
          # (chunk_size 4). Failing on write 3 therefore happens after the
          # POST body is fully on the wire and before the third request.
          :fail_on_write_call => 3
        )
      end
      let(:connection) { connection_with_socket(socket, :persistent => true) }

      it 'keeps the earlier response and marks the sent item unknown, later items not_sent' do
        results = connection.pipeline_requests([
          { :method => :get, :path => '/one' },
          { :method => :post, :path => '/two', :body => 'body-bytes',
            :chunk_size => 4 },
          { :method => :get, :path => '/three' }
        ])

        expect(result_statuses(results)).to eq([
          :completed, :sent_outcome_unknown, :not_sent
        ])
        expect(results[0][:response].body).to eq('one')
        expect(results[1][:error]).to be_a(Excon::Error::Socket)
        expect(results[2][:error]).to be_nil
        expect(socket.closed).to be(true)
      end
    end

    context 'when the connection drops halfway through the third response' do
      let(:socket) do
        truncated_third = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nabc"
        Excon::ScriptedPipelineSocket.new(
          :responses => [http_response('one'), http_response('two'), truncated_third]
        )
      end
      let(:connection) { connection_with_socket(socket, :persistent => true) }

      it 'saves the first two responses and marks only the half response unknown' do
        results = connection.pipeline_requests([
          { :path => '/1' }, { :path => '/2' }, { :path => '/3' }
        ])

        expect(result_statuses(results)).to eq([
          :completed, :completed, :sent_outcome_unknown
        ])
        expect(results[0][:response].body).to eq('one')
        expect(results[1][:response].body).to eq('two')
        expect(results[2][:response]).to be_nil
        expect(results[2][:error]).to be_a(Excon::Error::Socket)
        expect(socket.closed).to be(true)
      end
    end

    context 'when the read fails at a specified read position' do
      it 'fails the third item when the body read of the third response raises' do
        # Two full responses are scripted, followed by the status line of the
        # third. The read double is told to fail on the first body read call
        # for that third response.
        third_headers = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n"
        socket = Excon::ScriptedPipelineSocket.new(
          :responses => [http_response('one'), http_response('two'), third_headers],
          :fail_on_read_call => 3
        )
        connection = connection_with_socket(socket, :persistent => true)

        results = connection.pipeline_requests([
          { :path => '/1' }, { :path => '/2' }, { :path => '/3' }
        ])

        expect(result_statuses(results)).to eq([
          :completed, :completed, :sent_outcome_unknown
        ])
        expect(results[0][:response].body).to eq('one')
        expect(results[1][:response].body).to eq('two')
        expect(results[2][:error]).to be_a(Excon::Error::Socket)
        expect(socket.closed).to be(true)
      end
    end

    context 'when response parsing fails' do
      it 'retains the parse error and removes the connection from reuse' do
        malformed = "HTTP/1.1 200 OK\r\nContent-Length 2\r\n\r\nok"
        socket = Excon::ScriptedPipelineSocket.new(
          :responses => [http_response('one'), malformed]
        )
        connection = connection_with_socket(socket, :persistent => true)

        results = connection.pipeline_requests([{ :path => '/1' }, { :path => '/2' }])

        expect(result_statuses(results)).to eq(%i[completed sent_outcome_unknown])
        expect(results[0][:response].body).to eq('one')
        expect(results[1][:error]).to be_a(Excon::Error)
        expect(socket.closed).to be(true)
      end
    end

    context 'when a response middleware raises' do
      it 'retains the middleware error for that item and later items' do
        boom = Class.new(RuntimeError)
        middleware = Class.new(Excon::Middleware::Base) do
          define_method(:response_call) do |datum|
            raise boom.new('middleware failure') if datum[:path] == '/two'

            @stack.response_call(datum)
          end
        end

        socket = Excon::ScriptedPipelineSocket.new(
          :responses => [http_response('one'), http_response('two'), http_response('three')]
        )
        connection = connection_with_socket(
          socket, :persistent => true, :middlewares => Excon.defaults[:middlewares] + [middleware]
        )

        results = connection.pipeline_requests([
          { :path => '/one' }, { :path => '/two' }, { :path => '/three' }
        ])

        expect(result_statuses(results)).to eq([
          :completed, :sent_outcome_unknown, :sent_outcome_unknown
        ])
        expect(results[0][:response].body).to eq('one')
        expect(results[1][:error]).to be_a(boom)
        expect(results[2][:error]).to be_a(boom)
        expect(socket.closed).to be(true)
      end
    end

    context 'when write failure is reported even though the write accepted bytes' do
      it 'never downgrades an item to not_sent once bytes may have been sent' do
        socket = Excon::ScriptedPipelineSocket.new(
          :responses => [],
          :fail_after_written => 4,
          :fail_accept_bytes => 4
        )
        connection = connection_with_socket(socket, :persistent => true)

        results = connection.pipeline_requests([{ :path => '/one' }, { :path => '/two' }])

        expect(socket.bytes_written).to eq(4)
        expect(result_statuses(results)).to eq(%i[sent_outcome_unknown not_sent])
      end
    end

    describe 'caller-owned parameters and retries' do
      it 'does not mutate the caller param Hashes' do
        socket = Excon::ScriptedPipelineSocket.new(
          :responses => [http_response('one'), http_response('two')]
        )
        connection = connection_with_socket(socket, :persistent => true)
        first = { :method => :get, :path => '/one' }
        second = { :method => :get, :path => '/two' }
        first_snapshot = first.dup
        second_snapshot = second.dup

        connection.pipeline_requests([first, second])

        expect(first).to eq(first_snapshot)
        expect(second).to eq(second_snapshot)
      end

      it 'does not retry items whose outcome is unknown, even when idempotent' do
        socket = Excon::ScriptedPipelineSocket.new(
          :responses => [http_response('one')],
          # First GET writes successfully (call 1); the idempotent PUT write
          # (call 2) fails, so its outcome is unknown and it must not retry.
          :fail_on_write_call => 2
        )
        connection = connection_with_socket(socket, :persistent => true)

        results = connection.pipeline_requests([
          { :path => '/one' },
          { :method => :put, :path => '/two',
            :idempotent => true, :retry_limit => 5 }
        ])

        expect(result_statuses(results)).to eq(%i[completed sent_outcome_unknown])
        expect(results[1][:error]).to be_a(Excon::Error::Socket)
        # Exactly two write attempts: no automatic re-send of the PUT.
        expect(socket.write_calls).to eq(2)
      end
    end
  end
end
