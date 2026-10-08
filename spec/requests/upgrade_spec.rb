require 'spec_helper'

describe Excon::Connection do
  context 'HTTP Upgrade handshakes' do
    # Starts a loopback server that accepts a single connection, reads the
    # request head, and hands the client socket to the given block.
    # Returns [port, thread, request_head_queue].
    def start_loopback_server(&handler)
      server = TCPServer.new('127.0.0.1', 0)
      port = server.addr[1]
      request_head = Queue.new
      thread = Thread.new do
        client = server.accept
        head = +''
        while (line = client.gets) && line != "\r\n"
          head << line
        end
        request_head << head
        handler.call(client)
        client.close rescue nil
        server.close rescue nil
      end
      [port, thread, request_head]
    end

    [true, false].each do |nonblock|
      context "nonblock => #{nonblock}" do
        it 'hands over the socket, delivering pre-read bytes first' do
          prefix = "\x00\x01\x02\xFFecho-prefix".b
          echoed = Queue.new
          port, thread, request_head = start_loopback_server do |client|
            # single write: 101 response head followed by the binary prefix
            client.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: echo\r\nConnection: Upgrade\r\n\r\n" + prefix)
            echoed << client.read(4) # waits for the takeover to write
            client.write('pong')
            client.read # waits for the takeover to close
            echoed << nil
          end

          conn = Excon.new("http://127.0.0.1:#{port}", :nonblock => nonblock, :persistent => true)
          takeover = Timeout.timeout(10) { conn.upgrade(:headers => { 'Upgrade' => 'echo' }) }

          expect(takeover).to be_a(Excon::Upgrade)
          expect(takeover.socket).to be_a(Excon::Socket)

          head = request_head.pop
          expect(head).to include("Upgrade: echo\r\n")
          expect(head).to include("Connection: Upgrade\r\n")

          # the socket left the persistent pool, so reset cannot close it
          expect(conn.send(:sockets)).to be_empty
          conn.reset
          expect(takeover.closed?).to be(false)

          # the first byte read by the takeover is the first prefix byte,
          # followed by the rest of the prefix without loss or duplication
          expect(takeover.read(1)).to eq(prefix[0, 1])
          expect(takeover.read(prefix.bytesize - 1)).to eq(prefix[1..-1])

          # bytes written after the handshake arrive next, in order
          takeover.write('ping')
          expect(echoed.pop).to eq('ping')
          expect(takeover.read(4)).to eq('pong')

          # close is idempotent and solely responsible for closing
          takeover.close
          takeover.close
          expect(takeover.closed?).to be(true)
          expect { takeover.read(1) }.to raise_error(IOError)
          expect(echoed.pop).to be_nil # server saw EOF after close

          expect(thread.join(5)).not_to be_nil
        end

        it 'returns a normal response when the server declines the upgrade' do
          body = 'upgrade declined'
          port, thread, = start_loopback_server do |client|
            client.write("HTTP/1.1 200 OK\r\nContent-Length: #{body.bytesize}\r\n\r\n#{body}")
            client.read # waits for the connection to be reset
          end

          conn = Excon.new("http://127.0.0.1:#{port}", :nonblock => nonblock)
          response = Timeout.timeout(10) { conn.upgrade(:headers => { 'Upgrade' => 'echo' }) }

          expect(response).to be_a(Excon::Response)
          expect(response.status).to eq(200)
          expect(response.body).to eq(body)
          expect(thread.join(5)).not_to be_nil # non-persistent connection was reset
        end

        it 'treats a 101 with mismatched upgrade fields as a normal response' do
          port, thread, = start_loopback_server do |client|
            client.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: other\r\nConnection: Upgrade\r\n\r\n")
            client.read # waits for the connection to be reset
          end

          conn = Excon.new("http://127.0.0.1:#{port}", :nonblock => nonblock, :persistent => true)
          response = Timeout.timeout(10) { conn.upgrade(:headers => { 'Upgrade' => 'echo' }) }

          expect(response).to be_a(Excon::Response)
          expect(response.status).to eq(101)
          expect(conn.send(:sockets)).to be_empty
          expect(thread.join(5)).not_to be_nil # connection was reset, not pooled
        end
      end
    end

    it 'requires an Upgrade protocol' do
      conn = Excon.new('http://127.0.0.1:9')
      expect { conn.upgrade }.to raise_error(ArgumentError)
      expect { conn.upgrade(:headers => { 'Upgrade' => '' }) }.to raise_error(ArgumentError)
    end
  end
end
