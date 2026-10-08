require 'spec_helper'

describe Excon do
  context 'when upgrading a connection' do
    before(:all) do
      # binary prefix the server writes in the same write as the 101 head,
      # so the client may read it together with the response headers
      @prefix = "\x00\x01\x02\r\n\xFFbinary-prefix".b
      @requests = Queue.new
      @server = TCPServer.new('127.0.0.1', 0)
      @port = @server.addr[1]
      handler = lambda do |sock|
        begin
          request = String.new
          request << sock.readpartial(1024) until request.include?("\r\n\r\n")
          @requests << request
          case request[%r{^\S+ (\S+)}, 1]
          when '/upgrade'
            # write the 101 head and the new protocol's first bytes at once
            sock.write("HTTP/1.1 101 Switching Protocols\r\n" \
                       "Upgrade: echo\r\nConnection: Upgrade\r\n\r\n" + @prefix)
            # echo anything the client sends until it disconnects
            loop do
              data = sock.recv(1024)
              break if data.nil? || data.empty?

              sock.write(data)
            end
          when '/mismatch'
            sock.write("HTTP/1.1 101 Switching Protocols\r\n" \
                       "Upgrade: other\r\nConnection: Upgrade\r\n\r\n")
          else
            sock.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n" \
                       "Connection: close\r\n\r\nok")
          end
        rescue EOFError, IOError, SystemCallError
          # client went away
        ensure
          sock.close rescue nil
        end
      end
      @acceptor = Thread.new do
        loop { Thread.new(@server.accept, &handler) }
      end
    end

    after(:all) do
      @acceptor.kill
      @server.close rescue nil
    end

    it 'sends Connection and Upgrade headers for the requested protocol' do
      conn = Excon.new("http://127.0.0.1:#{@port}")
      conn.upgrade(:path => '/upgrade', :upgrade => 'echo').close
      request = @requests.pop
      expect(request).to match(/^Connection: Upgrade\r$/i)
      expect(request).to match(/^Upgrade: echo\r$/i)
    end

    it 'hands off the connection with pre-read bytes delivered first' do
      conn = Excon.new("http://127.0.0.1:#{@port}", :persistent => true)
      upgrade = conn.upgrade(:path => '/upgrade', :upgrade => 'echo')
      expect(upgrade).to be_a(Excon::Upgrade)

      # the first byte of the prefix must be the first byte read
      first = upgrade.read(1)
      expect(first.getbyte(0)).to eq(@prefix.getbyte(0))
      rest = upgrade.read(@prefix.bytesize - 1)
      expect(first + rest).to eq(@prefix)

      # the taken-over socket left the persistent pool
      expect(conn.send(:sockets)).to be_empty
      upgrade.close
    end

    it 'is not closed by Connection#reset and closes idempotently' do
      conn = Excon.new("http://127.0.0.1:#{@port}", :persistent => true)
      upgrade = conn.upgrade(:path => '/upgrade', :upgrade => 'echo')
      upgrade.read(@prefix.bytesize) # drain the prefix

      conn.reset
      expect(upgrade.closed?).to be(false)
      upgrade.write('ping')
      expect(upgrade.read(4)).to eq('ping')

      # the originating connection can be reused for new requests
      expect(conn.get(:path => '/other').body).to eq('ok')

      upgrade.close
      expect(upgrade.closed?).to be(true)
      expect { upgrade.close }.not_to raise_error
      upgrade.close
    end

    it 'returns a normal response when the server declines the upgrade' do
      conn = Excon.new("http://127.0.0.1:#{@port}")
      response = conn.upgrade(:path => '/decline', :upgrade => 'echo')
      expect(response).to be_a(Excon::Response)
      expect(response.status).to eq(200)
      expect(response.body).to eq('ok')
    end

    it 'returns a normal response when the 101 headers do not match' do
      conn = Excon.new("http://127.0.0.1:#{@port}", :persistent => true)
      response = conn.upgrade(:path => '/mismatch', :upgrade => 'echo')
      expect(response).to be_a(Excon::Response)
      expect(response.status).to eq(101)
      # the connection spoke an unexpected protocol, so it was not pooled
      expect(conn.send(:sockets)).to be_empty
    end

    it 'requires an :upgrade protocol' do
      conn = Excon.new("http://127.0.0.1:#{@port}")
      expect { conn.upgrade(:path => '/upgrade') }.to raise_error(ArgumentError)
    end
  end
end
