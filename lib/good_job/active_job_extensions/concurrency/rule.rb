# frozen_string_literal: true

module GoodJob
  module ActiveJobExtensions
    module Concurrency
      # Encapsulates a single concurrency control rule with its own label and limits
      class Rule
        VALID_TYPES = [String, Symbol, Numeric, Date, Time, TrueClass, FalseClass, NilClass].freeze

        attr_reader :config, :label_callable

        def initialize(config)
          @config = config.symbolize_keys
          @label_callable = @config[:label]
          validate_config!
        end

        # Evaluates the label in the context of a job instance
        # @param job [ActiveJob::Base] the job instance
        # @return [String, nil] the evaluated label
        def label(job)
          return job.class.name if label_callable.blank?

          label_value = label_callable.respond_to?(:call) ? job.instance_exec(&label_callable) : label_callable
          return if label_value.blank?

          raise TypeError, "Concurrency rule label must be a String; was a #{label_value.class}" unless VALID_TYPES.any? { |type| label_value.is_a?(type) }

          label_value.to_s
        end

        # Gets the total concurrency limit (shared by enqueue and perform)
        def total_limit(job)
          get_limit(job, :total_limit)
        end

        def enqueue_limit(job)
          get_limit(job, :enqueue_limit)
        end

        def perform_limit(job)
          get_limit(job, :perform_limit)
        end

        def enqueue_throttle(job)
          get_throttle(job, :enqueue_throttle)
        end

        def perform_throttle(job)
          get_throttle(job, :perform_throttle)
        end

        private

        def get_limit(job, key)
          value = @config[key]
          return if value.blank?

          value = job.instance_exec(&value) if value.respond_to?(:call)
          return unless value.present? && (0...Float::INFINITY).cover?(value)

          value
        end

        def get_throttle(job, key)
          value = @config[key]
          return if value.blank?

          value = job.instance_exec(&value) if value.respond_to?(:call)
          return unless value.present? && value.is_a?(Array) && value.size == 2

          value
        end

        def validate_config!
          # Ensure at least one limit or throttle is specified
          has_limits = @config.slice(:total_limit, :enqueue_limit, :perform_limit, :enqueue_throttle, :perform_throttle).any?
          return if has_limits

          raise ArgumentError, "Concurrency rule requires at least one limit or throttle option"
        end
      end
    end
  end
end
