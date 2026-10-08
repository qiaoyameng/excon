# frozen_string_literal: true

module Excon
  # A deterministic socket double for exercising Excon::Connection#pipeline_requests.
  #
  # Write-side failures are driven by byte position: as soon as the cumulative
  # number of bytes the caller attempts to write reaches +fail_after_written+
  # (optionally accepting +fail_accept_bytes+ of the crossing write), the
  # configured error is raised. +fail_on_write_call+ fails on an exact write
  # call instead. This lets a test fail halfway through request headers or
  # after a request body has been written.
  #
  # Read-side failures are driven by position in the scripted responses: simply
  # provide a response stream that ends (or becomes invalid) at the desired
  # boundary, e.g. a truncated third response after two complete ones.
  class ScriptedPipelineSocket
    attr_accessor :data
    attr_reader :closed, :bytes_written, :write_calls, :bytes_accepted_on_last_failed_write

    def initialize(responses:,
                   fail_after_written: nil,
                   fail_accept_bytes: nil,
                   fail_on_write_call: nil,
                   fail_on_read_call: nil,
                   fail_on_readline_call: nil,
                   write_error: Errno::ECONNRESET,
                   read_error: EOFError)
      @buffer = String.new(encoding: Encoding::BINARY)
      responses.each { |response| @buffer << response }
      @fail_after_written = fail_after_written
      @fail_accept_bytes = fail_accept_bytes
      @fail_on_write_call = fail_on_write_call
      @fail_on_read_call = fail_on_read_call
      @fail_on_readline_call = fail_on_readline_call
      @write_error = write_error
      @read_error = read_error
      @bytes_written = 0
      @write_calls = 0
      @read_calls = 0
      @readline_calls = 0
      @bytes_accepted_on_last_failed_write = nil
      @closed = false
    end

    def write(chunk)
      @write_calls += 1
      chunk = chunk.to_s
      size = chunk.bytesize

      if @fail_on_write_call && @write_calls == @fail_on_write_call
        @bytes_accepted_on_last_failed_write = nil
        raise @write_error.exception
      end

      if @fail_after_written && @bytes_written >= @fail_after_written
        @bytes_accepted_on_last_failed_write = 0
        raise @write_error.exception
      elsif @fail_after_written && @bytes_written + size > @fail_after_written
        accepted = @fail_after_written - @bytes_written
        accepted = [accepted, @fail_accept_bytes].min unless @fail_accept_bytes.nil?
        accepted = [accepted, 0].max
        @bytes_written += accepted
        @bytes_accepted_on_last_failed_write = accepted
        raise @write_error.exception
      end

      @bytes_written += size
      @bytes_accepted_on_last_failed_write = nil
      size
    end

    def readline
      @readline_calls += 1
      raise @read_error.exception if @fail_on_readline_call && @readline_calls == @fail_on_readline_call

      index = @buffer.index("\n")
      raise @read_error.exception if index.nil?

      @buffer.slice!(0, index + 1)
    end

    def read(length)
      @read_calls += 1
      raise @read_error.exception if @fail_on_read_call && @read_calls == @fail_on_read_call

      raise @read_error.exception if length && @buffer.empty?

      chunk = @buffer.slice!(0, [length, @buffer.bytesize].min)
      chunk.empty? ? nil : chunk
    end

    def close
      @closed = true
    end

    def remote_ip
      '127.0.0.1'
    end

    def local_port
      45678
    end

    def local_address
      '127.0.0.1'
    end
  end
end
