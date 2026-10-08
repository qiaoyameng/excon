# frozen_string_literal: true
module Excon
  # Excon::Upgrade is the result of a successful HTTP Upgrade handshake
  # (RFC 7230, section 6.7, see Connection#upgrade). It takes over the
  # underlying socket, which is removed from the connection's pool and is no
  # longer closed by Connection#reset; this object holds the sole
  # responsibility for closing it. Any bytes the response parser read beyond
  # the 101 response headers are delivered first, so no data is lost or
  # duplicated. It does not parse any subsequent protocol (e.g. WebSocket).
  class Upgrade
    # @return [Excon::Socket] the taken-over socket
    attr_reader :socket

    def initialize(socket, buffered = String.new)
      @socket = socket
      @buffered = buffered
      @closed = false
    end

    # Reads from the taken-over connection, delivering bytes buffered during
    # the handshake before newly arriving data.
    # @param [Integer, nil] max_length maximum number of bytes to read; reads
    #   until EOF when omitted
    # @return [String, nil] the data read, or nil on EOF when max_length given
    def read(max_length = nil)
      if max_length
        chunk = @buffered.slice!(0, max_length)
        if chunk.bytesize < max_length && (more = @socket.read(max_length - chunk.bytesize))
          chunk << more
        end
        chunk.empty? ? nil : chunk
      else
        @buffered.slice!(0, @buffered.bytesize) << (@socket.read || String.new)
      end
    end

    # Writes to the taken-over connection.
    def write(data)
      @socket.write(data)
    end

    # Closes the taken-over connection. Idempotent; subsequent calls are no-ops.
    def close
      unless @closed
        @closed = true
        begin
          @socket.close
        rescue StandardError
          nil
        end
      end
      nil
    end

    def closed?
      @closed
    end
  end
end
