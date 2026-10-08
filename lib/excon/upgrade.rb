# frozen_string_literal: true
module Excon
  # Takeover object returned by Excon::Connection#upgrade after a successful
  # HTTP Upgrade (101) handshake.
  #
  # The underlying socket has been removed from Excon's persistent socket
  # pool, so it will neither be reused for further HTTP requests nor closed
  # by Excon::Connection#reset. This object holds the sole responsibility
  # for closing it: #close is idempotent, and if the caller simply abandons
  # the object the socket is reclaimed by the garbage collector as usual.
  #
  # Bytes the response parser already read past the response head (the
  # beginning of the upgraded protocol) are delivered first by #read,
  # before any further bytes are read from the socket.
  class Upgrade
    # The underlying Excon::Socket the handshake was performed on.
    attr_reader :socket

    def initialize(socket, buffered = String.new)
      @socket = socket
      @buffered = buffered.dup
      @closed = false
    end

    # Bytes read past the response head that have not been consumed yet.
    def buffered
      @buffered.dup
    end

    # Reads up to +max_length+ bytes (or everything until EOF when omitted),
    # serving pre-read buffered bytes before reading from the socket.
    def read(max_length = nil)
      raise IOError, 'closed stream' if @closed

      if max_length
        return String.new if max_length == 0

        chunk = @buffered.slice!(0, max_length) || String.new
        remaining = max_length - chunk.bytesize
        rest = nil
        if remaining > 0
          rest = @socket.read(remaining)
          chunk << rest if rest
        end
        chunk.empty? && rest.nil? ? nil : chunk
      else
        data = @buffered.slice!(0, @buffered.bytesize) || String.new
        if (rest = @socket.read)
          data << rest
        end
        data
      end
    end

    def write(data)
      raise IOError, 'closed stream' if @closed

      @socket.write(data)
    end

    # Closes the underlying socket. Idempotent: subsequent calls are no-ops.
    def close
      return nil if @closed

      @closed = true
      @socket.close rescue nil
      nil
    end

    def closed?
      @closed
    end
  end
end
