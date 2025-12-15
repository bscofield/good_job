# frozen_string_literal: true

require_relative 'concurrency/rule'

module GoodJob
  module ActiveJobExtensions
    module Concurrency
      extend ActiveSupport::Concern

      VALID_TYPES = [String, Symbol, Numeric, Date, Time, TrueClass, FalseClass, NilClass].freeze

      class ConcurrencyExceededError < StandardError
        def backtrace
          [] # suppress backtrace
        end
      end

      ThrottleExceededError = Class.new(ConcurrencyExceededError)

      module Prepends
        def serialize(*)
          super.tap do |job_data|
            job_data['good_job_concurrency_key'] = good_job_concurrency_key if good_job_concurrency_key.present?
            job_data['good_job_concurrency_labels'] = good_job_concurrency_labels if good_job_concurrency_labels.present?
          end
        end

        def deserialize(job_data)
          super
          self.good_job_concurrency_key = job_data['good_job_concurrency_key']
          self.good_job_concurrency_labels = job_data['good_job_concurrency_labels'] || []
        end
      end

      included do
        prepend Prepends

        class_attribute :good_job_concurrency_config, instance_accessor: false, default: {}
        class_attribute :good_job_concurrency_rules, instance_accessor: false, default: []
        attr_writer :good_job_concurrency_key
        attr_accessor :good_job_concurrency_labels

        wait_key = if ActiveJob.gem_version >= Gem::Version.new("7.1.0.a")
                     :polynomially_longer
                   else
                     :exponentially_longer
                   end
        retry_on(
          GoodJob::ActiveJobExtensions::Concurrency::ConcurrencyExceededError,
          attempts: Float::INFINITY,
          wait: wait_key
        )

        before_enqueue do |job|
          # Don't attempt to enforce concurrency limits with other queue adapters.
          next unless job.class.queue_adapter.is_a?(GoodJob::Adapter)

          # Always allow jobs to be retried because the current job's execution will complete momentarily
          next if CurrentThread.active_job_id == job.job_id

          # Check all concurrency constraints (rules and legacy converted to rules)
          unless job.class.good_job_concurrency_rules.empty?
            job.good_job_concurrency_labels ||= job._good_job_concurrency_labels

            # For legacy rules, also set the concurrency_key so it gets persisted to the database
            # (needed for backward compatibility and database queries)
            legacy_rules = job.class.good_job_concurrency_rules.select(&:legacy?)
            job.good_job_concurrency_key ||= job._good_job_concurrency_key if legacy_rules.any?

            # Also set good_job_labels so Job model can persist labels to database (if Labels extension is included)
            job.good_job_labels = (job.good_job_labels || []) + job.good_job_concurrency_labels if job.respond_to?(:good_job_labels=)

            exceeded = nil
            job.class.good_job_concurrency_rules.each do |rule|
              rule_exceeded = job._check_enqueue_rule_concurrency(rule)
              if rule_exceeded
                exceeded = rule_exceeded
                break
              end
            end

            throw :abort if exceeded
          end
        end

        before_perform do |job|
          # Don't attempt to enforce concurrency limits with other queue adapters.
          next unless job.class.queue_adapter.is_a?(GoodJob::Adapter)

          # Check all concurrency constraints (rules and legacy converted to rules)
          unless job.class.good_job_concurrency_rules.empty?
            exceeded = nil
            job.class.good_job_concurrency_rules.each do |rule|
              rule_exceeded = job._check_perform_rule_concurrency(rule)
              if rule_exceeded
                exceeded = rule_exceeded
                break
              end
            end

            if exceeded == :limit
              raise GoodJob::ActiveJobExtensions::Concurrency::ConcurrencyExceededError
            elsif exceeded == :throttle
              raise GoodJob::ActiveJobExtensions::Concurrency::ThrottleExceededError
            end
          end
        end
      end

      class_methods do
        def good_job_control_concurrency_with(config)
          # Store for backward compatibility if needed
          self.good_job_concurrency_config = config

          # Convert legacy config to a LegacyRule and add it to the rules array
          # Legacy rules are prepended so they are checked first
          rule = LegacyRule.new(config)
          self.good_job_concurrency_rules = [rule] + good_job_concurrency_rules
        end

        def good_job_concurrency_rule(config)
          rule = Rule.new(config)
          self.good_job_concurrency_rules = good_job_concurrency_rules + [rule]
        end
      end

      # Existing or dynamically generated concurrency key
      # @return [Object] concurrency key
      def good_job_concurrency_key
        @good_job_concurrency_key || _good_job_concurrency_key
      end

      # Generates the concurrency key from the configuration
      # @return [Object] concurrency key
      def _good_job_concurrency_key
        return _good_job_default_concurrency_key unless self.class.good_job_concurrency_config.key?(:key)

        key = self.class.good_job_concurrency_config[:key]
        return if key.blank?

        key = instance_exec(&key) if key.respond_to?(:call)
        raise TypeError, "Concurrency key must be a String; was a #{key.class}" unless VALID_TYPES.any? { |type| key.is_a?(type) }

        key
      end

      # Generates the default concurrency key when the configuration doesn't provide one
      # @return [String] concurrency key
      def _good_job_default_concurrency_key
        self.class.name.to_s
      end

      # Generates concurrency labels from all configured rules
      # @return [Array<String>] array of labels for this job instance
      def _good_job_concurrency_labels
        self.class.good_job_concurrency_rules.filter_map do |rule|
          rule.label(self)
        end
      end

      # Checks if a rule's enqueue concurrency constraint is exceeded
      # @param rule [Rule] the concurrency rule to check
      # @return [Symbol, nil] :limit, :throttle, or nil if not exceeded
      def _check_enqueue_rule_concurrency(rule)
        enqueue_limit = rule.enqueue_limit(self)
        total_limit = rule.total_limit(self) unless enqueue_limit
        enqueue_throttle = rule.enqueue_throttle(self)

        limit = enqueue_limit || total_limit
        throttle = enqueue_throttle
        return nil unless limit || throttle

        labels = good_job_concurrency_labels
        return nil if labels.blank?

        constraint_type = rule.legacy? ? "concurrency key" : "concurrency label"
        query_scope = rule.query_scope

        exceeded = nil
        GoodJob::Job.transaction(requires_new: true, joinable: false) do
          labels.each do |label|
            GoodJob::Job.advisory_lock_key(label, function: "pg_advisory_xact_lock") do
              if limit
                enqueue_concurrency = if enqueue_limit
                                        query_scope.call(label).unfinished.advisory_unlocked.count
                                      else
                                        query_scope.call(label).unfinished.count
                                      end

                if (enqueue_concurrency + 1) > limit
                  logger.info "Aborted enqueue of #{self.class.name} (Job ID: #{job_id}) because the #{constraint_type} '#{label}' has reached its enqueue limit of #{limit} #{'job'.pluralize(limit)}"
                  exceeded = :limit
                  break
                end
              end

              if throttle
                throttle_limit = throttle[0]
                throttle_period = throttle[1]
                enqueued_within_period = query_scope.call(label)
                                                    .where(GoodJob::Job.arel_table[:created_at].gt(throttle_period.ago))
                                                    .count

                if (enqueued_within_period + 1) > throttle_limit
                  logger.info "Aborted enqueue of #{self.class.name} (Job ID: #{job_id}) because the #{constraint_type} '#{label}' has reached its throttle limit of #{throttle_limit} #{'job'.pluralize(throttle_limit)}"
                  exceeded = :throttle
                  break
                end
              end
            end

            break if exceeded
          end

          raise ActiveRecord::Rollback
        end

        exceeded
      end

      # Checks if a rule's perform concurrency constraint is exceeded
      # @param rule [Rule] the concurrency rule to check
      # @return [Symbol, nil] :limit, :throttle, or nil if not exceeded
      def _check_perform_rule_concurrency(rule)
        perform_limit = rule.perform_limit(self)
        total_limit = rule.total_limit(self) unless perform_limit
        perform_throttle = rule.perform_throttle(self)

        limit = perform_limit || total_limit
        throttle = perform_throttle
        return nil unless limit || throttle

        if CurrentThread.job.blank? || CurrentThread.job.active_job_id != job_id
          logger.debug("Ignoring concurrency limits because the job is executed with `perform_now`.")
          return nil
        end

        labels = good_job_concurrency_labels
        return nil if labels.blank?

        query_scope = rule.query_scope

        exceeded = nil
        GoodJob::Job.transaction(requires_new: true, joinable: false) do
          labels.each do |label|
            GoodJob::Job.advisory_lock_key(label, function: "pg_advisory_xact_lock") do
              if limit
                allowed_active_job_ids = query_scope.call(label).unfinished
                                                    .advisory_locked
                                                    .order(Arel.sql("COALESCE(performed_at, scheduled_at, created_at) ASC"))
                                                    .limit(limit).pluck(:active_job_id)

                unless allowed_active_job_ids.include?(job_id)
                  exceeded = :limit
                  break
                end
              end

              if throttle
                throttle_limit = throttle[0]
                throttle_period = throttle[1]

                # For legacy rules, query by concurrency_key directly; for new rules, use the label subquery
                if rule.legacy?
                  query = Execution.joins(:job)
                                   .where(GoodJob::Job.table_name => { concurrency_key: label })
                                   .where(Execution.arel_table[:created_at].gt(Execution.bind_value('created_at', throttle_period.ago, ActiveRecord::Type::DateTime)))
                else
                  job_scope = query_scope.call(label)
                  query = Execution.joins(:job)
                                   .where(GoodJob::Job.arel_table[:id].in(job_scope.select(:id)))
                                   .where(Execution.arel_table[:created_at].gt(Execution.bind_value('created_at', throttle_period.ago, ActiveRecord::Type::DateTime)))
                end

                allowed_active_job_ids = query.where(error: nil).or(query.where.not(error: "GoodJob::ActiveJobExtensions::Concurrency::ThrottleExceededError: GoodJob::ActiveJobExtensions::Concurrency::ThrottleExceededError"))
                                              .order(created_at: :asc)
                                              .limit(throttle_limit)
                                              .pluck(:active_job_id)

                unless allowed_active_job_ids.include?(job_id)
                  exceeded = :throttle
                  break
                end
              end
            end

            break if exceeded
          end

          raise ActiveRecord::Rollback
        end

        exceeded
      end
    end
  end
end
