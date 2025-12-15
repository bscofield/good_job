# frozen_string_literal: true

require 'rails_helper'

RSpec.describe 'GoodJob::ActiveJobExtensions::Concurrency with good_job_concurrency_rule' do
  before do
    ActiveJob::Base.queue_adapter = GoodJob::Adapter.new(execution_mode: :external)

    stub_const 'JOB_PERFORMED', Concurrent::AtomicBoolean.new(false)
  end

  describe '.good_job_concurrency_rule' do
    describe 'single rule with total_limit' do
      before do
        stub_const 'TestJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_concurrency_rule(
            total_limit: 2,
            label: -> { "rule-#{arguments.first}" }
          )

          def perform(name)
            name && sleep(1)
          end
        end)
      end

      it 'prevents enqueue when total_limit is exceeded' do
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Alice')).to be false
      end

      it 'allows different labels to be enqueued independently' do
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Bob')).to be_present
        expect(TestJob.perform_later('Bob')).to be_present
        expect(TestJob.perform_later('Alice')).to be false
        expect(TestJob.perform_later('Bob')).to be false
      end
    end

    describe 'single rule with enqueue_limit' do
      before do
        stub_const 'TestJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_concurrency_rule(
            enqueue_limit: 3,
            label: -> { "enqueue-#{arguments.first}" }
          )

          def perform(name)
            name
          end
        end)
      end

      it 'prevents enqueue when enqueue_limit is exceeded' do
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Alice')).to be false
      end

      it 'excludes locked jobs from the count' do
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Alice')).to be_present

        # Lock one of the jobs
        Rails.application.executor.wrap do
          GoodJob::Job.first.with_advisory_lock do
            # Should allow one more because one is locked
            expect(TestJob.perform_later('Alice')).to be_present
          end
        end
      end
    end

    describe 'single rule with enqueue_throttle' do
      before do
        allow(GoodJob).to receive(:preserve_job_records).and_return(true)

        stub_const 'TestJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_concurrency_rule(
            enqueue_throttle: [1, 1.minute],
            label: -> { "throttle-#{arguments.first}" }
          )

          def perform(name)
            name
          end
        end)
      end

      it 'prevents enqueue if throttle period has not passed' do
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Alice')).to be false
        Timecop.travel(61.seconds.from_now) do
          expect(TestJob.perform_later('Alice')).to be_present
        end
      end
    end

    describe 'single rule via multiple calls' do
      before do
        stub_const 'TestJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_concurrency_rule(
            total_limit: 1,
            label: -> { "per-user-#{arguments.first}" }
          )

          def perform(user)
            user
          end
        end)
      end

      it 'enforces single rule correctly' do
        # Max 1 per user
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Alice')).to be false

        # Different user should be allowed
        expect(TestJob.perform_later('Bob')).to be_present
        expect(TestJob.perform_later('Bob')).to be false
      end
    end

    describe 'rule with dynamic label evaluation' do
      before do
        stub_const 'TestJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_concurrency_rule(
            total_limit: 1,
            label: -> { "user-#{arguments.first[:id]}" }
          )

          def perform(user)
            user[:id]
          end
        end)
      end

      it 'evaluates label dynamically based on job arguments' do
        user1 = { id: 1, name: 'Alice' }
        user2 = { id: 2, name: 'Bob' }

        expect(TestJob.perform_later(user1)).to be_present
        expect(TestJob.perform_later(user1)).to be false
        expect(TestJob.perform_later(user2)).to be_present
        expect(TestJob.perform_later(user2)).to be false
      end
    end

    describe 'rule with static label' do
      before do
        stub_const 'TestJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_concurrency_rule(
            total_limit: 2,
            label: 'static-label'
          )

          def perform(name)
            name
          end
        end)
      end

      it 'uses the static label for all jobs' do
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Bob')).to be_present
        # All jobs use the same static label, so third should be blocked
        expect(TestJob.perform_later('Charlie')).to be false
      end
    end

    describe 'rule without label (defaults to class name)' do
      before do
        stub_const 'TestJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_concurrency_rule(
            total_limit: 2
          )

          def perform(name)
            name
          end
        end)
      end

      it 'uses class name as default label' do
        expect(TestJob.perform_later('Alice')).to be_present
        expect(TestJob.perform_later('Bob')).to be_present
        expect(TestJob.perform_later('Charlie')).to be false
      end
    end

    describe 'rule with perform_limit' do
      before do
        allow(GoodJob).to receive(:preserve_job_records).and_return(true)

        stub_const 'TestJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_concurrency_rule(
            perform_limit: 1,
            label: -> { "perform-#{arguments.first}" }
          )

          def perform(name)
            # no-op
          end
        end)
      end

      it 'allows jobs with perform_limit configured' do
        # Just verify that perform_limit can be configured without errors
        job = TestJob.perform_later('Alice')
        expect(job).to be_present
        expect(GoodJob::Job.find_by(active_job_id: job.job_id)).to be_present
      end
    end

    describe 'rule with perform_throttle' do
      before do
        allow(GoodJob).to receive(:preserve_job_records).and_return(true)

        stub_const 'TestJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_concurrency_rule(
            perform_throttle: [2, 1.minute],
            label: -> { "throttle-#{arguments.first}" }
          )

          def perform(name)
            # no-op
          end
        end)
      end

      it 'allows jobs with perform_throttle configured' do
        # Just verify that perform_throttle can be configured without errors
        job = TestJob.perform_later('Alice')
        expect(job).to be_present
        expect(GoodJob::Job.find_by(active_job_id: job.job_id)).to be_present
      end
    end

    describe 'mixing legacy config and new rules' do
      before do
        stub_const 'TestJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_control_concurrency_with(
            total_limit: 1,
            key: -> { "legacy-#{arguments.first}" }
          )

          good_job_concurrency_rule(
            enqueue_throttle: [2, 1.minute],
            label: -> { "rule-#{arguments.first}" }
          )

          def perform(name)
            name
          end
        end)
      end

      it 'enforces both legacy and rule constraints' do
        # First job passes both checks
        expect(TestJob.perform_later('Alice')).to be_present

        # Legacy config limit (1) is reached, but rule allows 2
        # So it should be blocked by legacy config
        expect(TestJob.perform_later('Alice')).to be false
      end
    end

    describe 'rule label serialization and deserialization' do
      before do
        stub_const 'TestJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_concurrency_rule(
            total_limit: 1,
            label: -> { "rule-#{arguments.first}" }
          )

          def perform(name)
            name
          end
        end)
      end

      it 'persists and restores labels correctly' do
        job = TestJob.perform_later('Alice')
        db_job = GoodJob::Job.find_by(active_job_id: job.job_id)

        expect(db_job.labels).to eq(['rule-Alice'])

        # Deserialize and check labels are restored
        deserialized = ActiveJob::Base.deserialize(db_job.serialized_params)
        expect(deserialized.good_job_concurrency_labels).to eq(['rule-Alice'])
      end

      it 'preserves labels when retrying' do
        stub_const 'RetryJob', (Class.new(ActiveJob::Base) do
          include GoodJob::ActiveJobExtensions::Labels
          include GoodJob::ActiveJobExtensions::Concurrency

          good_job_concurrency_rule(
            total_limit: 1,
            label: -> { Time.current.to_f.to_s }
          )

          retry_on StandardError

          def perform
            raise "ERROR"
          end
        end)

        RetryJob.set(wait_until: 5.minutes.ago).perform_later

        begin
          GoodJob.perform_inline
        rescue StandardError
          nil
        end

        expect(GoodJob::Job.count).to eq 1
        expect(GoodJob::Job.first.labels).to be_present
        expect(GoodJob::Job.first).not_to be_finished
      end
    end
  end
end
