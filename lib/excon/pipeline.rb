# frozen_string_literal: true

module Excon
  # Partial-result HTTP/1.1 pipelining.
  #
  # Unlike {Connection#requests}, which raises on the first socket failure and
  # discards everything that happened before it, the pipeline entry points in
  # this module report one result per input request, in input order.
  module Pipeline
    # The full response was received and the response middlewares completed.
    COMPLETED = :completed

    # Request bytes may have reached the wire, but no complete response was
    # obtained. The request must not be retried blindly.
    SENT_OUTCOME_UNKNOWN = :sent_outcome_unknown

    # No request bytes could have been sent, so the request was never delivered.
    NOT_SENT = :not_sent

    STATUSES = [COMPLETED, SENT_OUTCOME_UNKNOWN, NOT_SENT].freeze

    # Installed on the socket used by a retaining pipeline so that the exact
    # point at which request bytes started going out is observable.
    module WriteMarker
      attr_accessor :pipeline_write_callback

      def write(data)
        if (callback = pipeline_write_callback)
          callback.call(data)
        end
        super
      end
    end

    # Outcome of a single request within a retaining pipeline batch.
    class Result
      # Zero-based position of the request in the input array.
      attr_reader :index

      # One of {Excon::Pipeline::COMPLETED},
      # {Excon::Pipeline::SENT_OUTCOME_UNKNOWN} or
      # {Excon::Pipeline::NOT_SENT}.
      attr_reader :status

      # Excon::Response when status is completed, nil otherwise.
      attr_reader :response

      # Exception that stopped the pipeline, retained for every result that is
      # not completed; nil for completed results.
      attr_reader :error

      def initialize(index, status, response: nil, error: nil)
        @index = index
        @status = status
        @response = response
        @error = error
      end

      def completed?
        status == COMPLETED
      end

      def sent_outcome_unknown?
        status == SENT_OUTCOME_UNKNOWN
      end

      def not_sent?
        status == NOT_SENT
      end

      def to_h
        hash = {
          :index => index,
          :status => status
        }
        hash[:response] = response unless response.nil?
        hash[:error] = error unless error.nil?
        hash
      end
      alias to_hash to_h

      def [](key)
        to_h[key]
      end

      def inspect
        "#<#{self.class} index=#{index.inspect} status=#{status.inspect} " \
          "response=#{response.inspect} error=#{error.inspect}>"
      end
    end
  end

  # Convenience alias for callers that prefer a flat constant namespace.
  PipelineResult = Pipeline::Result unless const_defined?(:PipelineResult)
end
