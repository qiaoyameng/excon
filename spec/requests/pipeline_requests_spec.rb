require 'spec_helper'

# Socket double that can fail at a precise write or read position.
#
# * +fail_after_write_bytes+: total request bytes that may be written before
#   the next write raises (nil never fails writes).
# * +fail_after_requests+: number of full requests after which the next write
#   fails partway through.
# * +read_queue+: strings served by subsequent read/readline calls; an
#   exception entry is raised (EOFError models a mid-read disconnect).
#
# The double implements only the surface Excon uses while parsing responses.
class FailingPipelineSocket
  attr_accessor :pipeline_write_callback
  attr_accessor :data
  attr_reader :written, :closed

  REQUEST_LINE = /[A-Z]+ \S+ HTTP\/1\.1\r\n/

  def initialize(read_queue: [], fail_after_write_bytes: nil,
                 fail_after_requests: nil, partial_request_bytes: 10,
                 write_error: nil)
    @read_queue = read_queue.dup
    @fail_after_write_bytes = fail_after_write_bytes
    @fail_after_requests = fail_after_requests
    @partial_request_bytes = partial_request_bytes
    @write_error = write_error || Errno::ECONNRESET.new
    @written = +''
    @closed = false
  end

  def write(data)
    data = data.to_s

    pipeline_write_callback.call(data) if pipeline_write_callback

    bytes = allowed_bytes(data)
    if bytes < data.bytesize
      @written << data.byteslice(0, bytes) if bytes > 0
      raise @write_error
    end

    @written << data
    data.bytesize
  end

  def allowed_bytes(data)
    if @fail_after_requests
      complete = @written.scan(REQUEST_LINE).length + data.scan(REQUEST_LINE).length
      if complete > @fail_after_requests
        return [@partial_request_bytes, data.bytesize].min
      end
    end

    if @fail_after_write_bytes
      remaining = @fail_after_write_bytes - @written.bytesize
      return [remaining, data.bytesize].min
    end

    data.bytesize
  end

  def next_read
    entry = @read_queue.shift
    raise EOFError if entry.nil?
    raise entry if entry.is_a?(Exception)

    entry
  end

  def readline
    next_read
  end

  def read(max_length = nil)
    next_read
  end

  def remote_ip
    '127.0.0.1'
  end

  def local_address
    '127.0.0.1'
  end

  def local_port
    42_000
  end

  def close
    @closed = true
  end
end

describe Excon::Connection, '#pipeline_requests' do
  let(:connection) do
    Excon::Connection.new(
      :host => '127.0.0.1',
      :hostname => '127.0.0.1',
      :port => 80,
      :scheme => 'http',
      :nonblock => false,
      :persistent => true
    )
  end

  def http_response(body: 'ok')
    [
      "HTTP/1.1 200 OK\r\n",
      "Content-Length: #{body.bytesize}\r\n",
      "\r\n",
      body
    ]
  end

  def stub_socket(socket)
    allow(connection).to receive(:socket) do |datum = nil|
      key = connection.instance_variable_get(:@socket_key)
      stored = connection.send(:sockets)[key] ||= socket
      stored.data = datum if datum
      stored
    end
    socket
  end

  def request_params(path = '/', body = nil)
    params = { :method => :get, :path => path }
    params[:body] = body unless body.nil?
    params
  end

  def statuses(results)
    results.map(&:status)
  end

  it 'returns an empty array for an empty pipeline' do
    expect(connection.pipeline_requests([])).to eq([])
  end

  it 'completes every request when the full pipeline succeeds' do
    socket = stub_socket(FailingPipelineSocket.new(
      :read_queue => http_response + http_response + http_response
    ))

    results = connection.pipeline_requests([request_params, request_params, request_params])

    expect(statuses(results)).to eq([:completed, :completed, :completed])
    expect(results.map { |result| result.response.body }).to eq(%w[ok ok ok])
    expect(results.map(&:index)).to eq([0, 1, 2])
    expect(socket.closed).to eq(false)
  end

  context 'when request headers are sent halfway' do
    it 'marks the in-flight item unknown and later items not sent' do
      socket = stub_socket(FailingPipelineSocket.new(
        :read_queue => [],
        :fail_after_write_bytes => 20
      ))

      results = connection.pipeline_requests([request_params, request_params, request_params])

      expect(statuses(results)).to eq(
        [:sent_outcome_unknown, :not_sent, :not_sent]
      )
      expect(results[0].error).to be_a(Excon::Errors::SocketError)
      expect(results[0].error.socket_error).to be_a(Errno::ECONNRESET)
      expect(results[0].response).to be_nil
      expect(results[1].error).to be_a(Excon::Errors::SocketError)
      expect(results[2].error).to be_a(Excon::Errors::SocketError)
      expect(socket.closed).to eq(true)
    end
  end

  context 'when the second item fails after the first body was sent' do
    it 'completes the first item, marks the in-flight item unknown and the rest not sent' do
      params = [request_params('/first', 'x' * 100), request_params, request_params]
      socket = stub_socket(FailingPipelineSocket.new(
        :read_queue => http_response,
        :fail_after_requests => 1
      ))

      results = connection.pipeline_requests(params)

      expect(statuses(results)).to eq(
        [:completed, :sent_outcome_unknown, :not_sent]
      )
      expect(results[0].response.body).to eq('ok')
      expect(results[1].error).to be_a(Excon::Errors::SocketError)
      expect(results[2].status).to eq(:not_sent)
      expect(socket.closed).to eq(true)
    end

    it 'keeps the connection out of the reuse pool after failure' do
      stub_socket(FailingPipelineSocket.new(
        :read_queue => http_response,
        :fail_after_requests => 1
      ))

      connection.pipeline_requests(
        [request_params('/first', 'x' * 100), request_params, request_params]
      )

      expect(connection.send(:sockets)).to be_empty
    end
  end

  context 'when the read fails after the second response' do
    it 'completes the first two items and marks the third unknown' do
      socket = stub_socket(FailingPipelineSocket.new(
        :read_queue => http_response + http_response + [EOFError.new]
      ))

      results = connection.pipeline_requests([request_params, request_params, request_params])

      expect(statuses(results)).to eq(
        [:completed, :completed, :sent_outcome_unknown]
      )
      expect(results[0].response.body).to eq('ok')
      expect(results[1].response.body).to eq('ok')
      expect(results[2].response).to be_nil
      expect(results[2].error).to be_a(Excon::Errors::SocketError)
      expect(socket.closed).to eq(true)
    end

    it 'does not complete an item when only half a response arrived' do
      socket = stub_socket(FailingPipelineSocket.new(
        :read_queue => http_response + http_response + [
          "HTTP/1.1 200 OK\r\n",
          "Content-Length: 10\r\n",
          "\r\n",
          'short',
          EOFError.new
        ]
      ))

      results = connection.pipeline_requests([request_params, request_params, request_params])

      expect(statuses(results)).to eq(
        [:completed, :completed, :sent_outcome_unknown]
      )
      expect(results[2].response).to be_nil
      expect(socket.closed).to eq(true)
    end

    it 'retains a response parse failure rather than a partial response' do
      socket = stub_socket(FailingPipelineSocket.new(
        :read_queue => http_response + [
          "HTTP/1.1 200 OK\r\n",
          "not-a-valid-header\r\n",
          "\r\n"
        ]
      ))

      results = connection.pipeline_requests([request_params, request_params])

      expect(statuses(results)).to eq([:completed, :sent_outcome_unknown])
      expect(results[1].response).to be_nil
      expect(results[1].error).to be_a(Excon::Errors::SocketError)
      expect(results[1].error.socket_error).to be_a(Excon::Error::ResponseParse)
      expect(socket.closed).to eq(true)
    end
  end

  context 'when a request middleware raises before any byte is written' do
    it 'keeps that item and every later item not_sent' do
      boom = Class.new(StandardError)
      middleware = Class.new(Excon::Middleware::Base) do
        define_method(:request_call) do |datum|
          raise boom.new('never reached the socket') if datum[:path] == '/boom'
          @stack.request_call(datum)
        end
      end

      socket = stub_socket(FailingPipelineSocket.new(:read_queue => []))
      params = [
        request_params('/boom'),
        request_params
      ].map { |item| item.merge(:middlewares => Excon.defaults[:middlewares] + [middleware]) }

      results = connection.pipeline_requests(params)

      expect(statuses(results)).to eq([:not_sent, :not_sent])
      expect(results[0].error).to be_a(boom)
      expect(results[1].error).to be_a(boom)
      expect(socket.written).to eq('')
      expect(socket.closed).to eq(true)
    end
  end

  it 'keeps the socket reusable after a fully successful pipeline' do
    socket = stub_socket(FailingPipelineSocket.new(
      :read_queue => http_response
    ))

    connection.pipeline_requests([request_params])

    expect(socket.closed).to eq(false)
    expect(connection.send(:sockets)).not_to be_empty
  end

  it 'retains a response middleware error as the reason' do
    boom = Class.new(StandardError)
    middleware = Class.new(Excon::Middleware::Base) do
      define_method(:response_call) do |datum|
        raise boom.new('middleware blew up') if datum[:path] == '/boom'
        @stack.response_call(datum)
      end
    end

    params = [
      request_params,
      request_params('/boom'),
      request_params
    ].map { |item| item.merge(:middlewares => Excon.defaults[:middlewares] + [middleware]) }

    socket = stub_socket(FailingPipelineSocket.new(
      :read_queue => http_response + http_response
    ))

    results = connection.pipeline_requests(params)

    expect(statuses(results)).to eq(
      [:completed, :sent_outcome_unknown, :sent_outcome_unknown]
    )
    expect(results[1].error).to be_a(Excon::Errors::SocketError)
    expect(results[1].error.socket_error).to be_a(boom)
    expect(socket.closed).to eq(true)
  end

  it 'does not mutate the caller params' do
    stub_socket(FailingPipelineSocket.new(
      :read_queue => http_response + http_response
    ))

    params = [request_params, request_params]
    originals = params.map(&:dup)

    connection.pipeline_requests(params)

    expect(params).to eq(originals)
  end

  it 'does not automatically resend items whose outcome is unknown' do
    socket = FailingPipelineSocket.new(
      :read_queue => [],
      :fail_after_write_bytes => 20
    )
    stub_socket(socket)

    connection.pipeline_requests([
      request_params.merge(:idempotent => true, :retry_limit => 5),
      request_params
    ])

    # The first request started writing once; no retried copies were sent.
    expect(socket.written.scan("HTTP/1.1").length).to eq(1)
  end

  it 'exposes hash-style result details' do
    stub_socket(FailingPipelineSocket.new(
      :read_queue => http_response,
      :fail_after_requests => 1
    ))

    results = connection.pipeline_requests(
      [request_params('/first', 'x' * 100), request_params]
    )

    expect(results[0][:status]).to eq(:completed)
    expect(results[0][:status]).to eq(Excon::Pipeline::COMPLETED)
    expect(results[0][:response]).to be_a(Excon::Response)
    expect(results[1][:status]).to eq(:sent_outcome_unknown)
    expect(results[1][:error]).to be_a(Excon::Errors::SocketError)
    expect(results.map(&:to_h).map { |item| item[:index] }).to eq([0, 1])
  end
end
