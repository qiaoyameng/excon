# frozen_string_literal: true

module Excon
  # Wraps the socket used by a #pipeline_requests run. It behaves exactly like
  # the underlying socket but remembers which request Datum was active each
  # time bytes were written. Once a write happens the corresponding item can no
  # longer be classified as +not_sent+, even if that write later raises.
  class PipelineSocket
    attr_accessor :current_datum
    attr_reader :inner_socket

    def initialize(socket)
      @socket = socket
      @sent_indices = {}
      @accepted_bytes = Hash.new(0)
    end

    def write(data)
      index = current_index
      # Bytes may reach the peer as soon as the underlying write is entered, so
      # mark the item optimistically. The rescue below downgrades it to unsent
      # only when the socket proves that zero bytes were accepted.
      mark_sent
      written =
        begin
          @socket.write(data)
        rescue StandardError
          if @socket.respond_to?(:bytes_accepted_on_last_failed_write)
            accepted = @socket.bytes_accepted_on_last_failed_write
            if accepted.nil?
              # Unknown how much left the process: assume bytes were sent.
            elsif accepted.positive?
              @accepted_bytes[index] += accepted
            elsif index && @accepted_bytes[index].zero?
              @sent_indices.delete(index)
            end
          end
          raise
        end

      if written.to_i.positive?
        @accepted_bytes[index] += written.to_i if index
        mark_sent
      end
      written
    end

    # Any other socket method (read, readline, close, remote_ip, ...) is
    # forwarded unchanged.
    def method_missing(method_name, *args, &block)
      @socket.send(method_name, *args, &block)
    end

    def respond_to_missing?(method_name, include_private = false)
      @socket.respond_to?(method_name, include_private) || super
    end

    def data=(datum)
      @socket.data = datum
    end

    # True once any bytes for +datum+ may have reached the peer.
    def sent?(datum)
      datum && @sent_indices.key?(datum[:pipeline_index])
    end

    private

    def mark_sent
      return unless @current_datum

      @sent_indices[@current_datum[:pipeline_index]] = true
    end

    def current_index
      @current_datum && @current_datum[:pipeline_index]
    end
  end
end
