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

        # Returns a proc that applies the appropriate query scope for this rule type
        # For standard rules, queries by labels array; for legacy rules, queries by concurrency_key column
        # @param base_scope [Class] the base GoodJob::Job scope to build on
        # @return [Proc] a proc that takes a label and returns a scoped query
        def query_scope(base_scope = GoodJob::Job)
          ->(label) { base_scope.where("? = ANY(labels)", label) }
        end

        # Indicates whether this is a legacy rule converted from good_job_control_concurrency_with
        # @return [Boolean] true if legacy, false otherwise
        def legacy?
          false
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

      # Legacy rule adapter: wraps good_job_control_concurrency_with config as a Rule
      # Allows legacy concurrency_key system to use the unified rule-based checking infrastructure
      class LegacyRule < Rule
        def initialize(config)
          super
          @key_callable = @config[:key]
        end

        # Legacy rules use the concurrency_key value as the label
        # @param job [ActiveJob::Base] the job instance
        # @return [String, nil] the evaluated concurrency key
        def label(job)
          return job.class.name if @key_callable.blank?

          key_value = @key_callable.respond_to?(:call) ? job.instance_exec(&@key_callable) : @key_callable
          return if key_value.blank?

          raise TypeError, "Concurrency key must be a String; was a #{key_value.class}" unless VALID_TYPES.any? { |type| key_value.is_a?(type) }

          key_value.to_s
        end

        # Legacy rules query by the concurrency_key column, not the labels array
        # @param base_scope [Class] the base GoodJob::Job scope to build on
        # @return [Proc] a proc that takes a concurrency_key and returns a scoped query
        def query_scope(base_scope = GoodJob::Job)
          ->(key) { base_scope.where(concurrency_key: key) }
        end

        # Indicates this is a legacy rule
        # @return [Boolean] always true for legacy rules
        def legacy?
          true
        end

        private

        def validate_config!
          # Legacy rules require at least one limit or throttle (from parent)
          # but do NOT require a label since it's derived from the key
          super
        end
      end
    end
  end
end
