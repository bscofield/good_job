# Unifying Legacy & Rules-Based Concurrency Checking
## Plan: Eliminate Code Duplication by Converting Legacy Config to Internal Rules

---

## Problem Statement

The current implementation has **two separate constraint-checking systems**:

1. **Rules-based** (`good_job_concurrency_rules`): Uses labels, checked via `_check_enqueue_rule_concurrency` / `_check_perform_rule_concurrency`
2. **Legacy** (`good_job_control_concurrency_with`): Uses concurrency_key, checked via inline logic in callbacks (~60 lines of duplicated code)

**The Issue:**
- Almost identical logic exists in 4 places:
  - `before_enqueue` legacy check (lines 78-124) 
  - `before_perform` legacy check (lines 180-229)
  - `_check_enqueue_rule_concurrency` (lines 283-320)
  - `_check_perform_rule_concurrency` (lines 329-380)

**Key Differences:**
- Legacy uses database column `concurrency_key` + direct advisory locks
- Rules use array column `labels` + multiple locks per job
- Legacy uses `:limit` and `:throttle` symbols
- Rules support dynamic label evaluation via lambdas

---

## Refactoring Strategy: Convert Legacy to Internal Rules

### Core Concept
**Convert legacy `good_job_concurrency_config` into an internal Rule at initialization time.**

Instead of checking legacy config separately in callbacks, synthesize a Rule object that the existing rule-checking infrastructure can handle.

### Implementation Overview

```
┌─ Job Class Definition ──────────────────────┐
│  good_job_control_concurrency_with(config)  │
│           ↓                                   │
│  (Convert to Rule)                          │
└─────────────────────────────────────────────┘
           ↓
┌─ Internal Rules Array ──────────────────────┐
│  good_job_concurrency_rules =               │
│    [LegacyRule, UserRule1, UserRule2, ...] │
└─────────────────────────────────────────────┘
           ↓
┌─ Unified Checking ──────────────────────────┐
│  before_enqueue:                            │
│    rules.each do |rule|                     │
│      _check_enqueue_rule_concurrency(rule)  │
│  before_perform:                            │
│    rules.each do |rule|                     │
│      _check_perform_rule_concurrency(rule)  │
└─────────────────────────────────────────────┘
```

---

## Phase 1: Create LegacyRule Adapter Class

### Purpose
Wrap legacy `good_job_concurrency_config` in a Rule-compatible interface.

### Design

```ruby
class LegacyRule < Rule
  # Inherits from Rule but overrides label/limit behavior
  
  def initialize(config)
    super(config)
    @is_legacy = true
  end
  
  # Legacy rules use concurrency_key as the label
  # instead of a computed/dynamic label
  def label(job)
    key = config[:key]
    return if key.blank?
    
    key = job.instance_exec(&key) if key.respond_to?(:call)
    key.to_s
  end
  
  # Label should default to job class name if no key specified
  def default_label(job)
    job.class.name.to_s
  end
  
  # Mark this as legacy so we know to handle it differently
  def legacy?
    true
  end
end
```

### Key Differences from Standard Rule
- `label()` uses the concurrency_key value (not a separate `label:` config)
- No support for dynamic labels (legacy configs don't have that)
- Needs to bridge to `concurrency_key` database column

---

## Phase 2: Convert Legacy Config to LegacyRule

### Location
Update the `good_job_control_concurrency_with` class method:

```ruby
class_methods do
  def good_job_control_concurrency_with(config)
    # Legacy: store for backward compatibility if needed
    self.good_job_concurrency_config = config
    
    # NEW: Convert to internal rule
    rule = LegacyRule.new(config)
    self.good_job_concurrency_rules = good_job_concurrency_rules.unshift(rule)
  end
  
  def good_job_concurrency_rule(config)
    # Existing code unchanged
    rule = Rule.new(config)
    self.good_job_concurrency_rules = good_job_concurrency_rules + [rule]
  end
end
```

### Benefits
- Legacy rules are now just another rule in the rules array
- Check all rules uniformly
- Both systems use same checking logic

---

## Phase 3: Update Database Query Logic

### Challenge
Legacy rules query by `concurrency_key` column, not labels array.

### Solution: Rule-aware Querying

Add a method to Rule to specify query strategy:

```ruby
class Rule
  # Standard: query by label array
  def query_scope(base_scope = GoodJob::Job)
    ->(label) { base_scope.where("? = ANY(labels)", label) }
  end
  
  def legacy?
    false
  end
end

class LegacyRule < Rule
  # Legacy: query by concurrency_key column
  def query_scope(base_scope = GoodJob::Job)
    ->(label) { base_scope.where(concurrency_key: label) }
  end
  
  def legacy?
    true
  end
end
```

### Updated Checking Logic

```ruby
def _check_enqueue_rule_concurrency(rule)
  enqueue_limit = rule.enqueue_limit(self)
  total_limit = rule.total_limit(self) unless enqueue_limit
  enqueue_throttle = rule.enqueue_throttle(self)
  
  limit = enqueue_limit || total_limit
  throttle = enqueue_throttle
  return nil unless limit || throttle
  
  labels = [rule.label(self)]  # Single label per rule (even legacy)
  return nil if labels.blank?
  
  exceeded = nil
  GoodJob::Job.transaction(requires_new: true, joinable: false) do
    labels.each do |label|
      GoodJob::Job.advisory_lock_key(label, function: "pg_advisory_xact_lock") do
        query_scope = rule.query_scope  # Gets rule-appropriate scope
        
        if limit
          enqueue_concurrency = if enqueue_limit
                                  query_scope.call(label).unfinished.advisory_unlocked.count
                                else
                                  query_scope.call(label).unfinished.count
                                end
          
          if (enqueue_concurrency + 1) > limit
            logger.info "..."
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
            logger.info "..."
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
```

---

## Phase 4: Remove Legacy Callback Code

### Before
```ruby
before_enqueue do |job|
  next unless job.class.queue_adapter.is_a?(GoodJob::Adapter)
  next if CurrentThread.active_job_id == job.job_id
  
  # Check rules-based
  unless job.class.good_job_concurrency_rules.empty?
    # 10 lines
  end
  
  # Check LEGACY  ← DELETE ALL THIS
  job.good_job_concurrency_key ||= job._good_job_concurrency_key
  key = job.good_job_concurrency_key
  next if key.blank?
  # ... 45 more lines of legacy logic
end
```

### After
```ruby
before_enqueue do |job|
  next unless job.class.queue_adapter.is_a?(GoodJob::Adapter)
  next if CurrentThread.active_job_id == job.job_id
  
  job.good_job_concurrency_labels ||= job._good_job_concurrency_labels
  job.good_job_labels = (job.good_job_labels || []) + job.good_job_concurrency_labels
  
  exceeded = job.class.good_job_concurrency_rules.find do |rule|
    rule_exceeded = job._check_enqueue_rule_concurrency(rule)
    rule_exceeded
  end
  
  throw :abort if exceeded
end
```

### Results
- **Before**: ~130 lines of before_enqueue logic
- **After**: ~15 lines of before_enqueue logic
- Same for before_perform: ~65 lines → ~15 lines

---

## Phase 5: Handle Edge Cases

### 1. Legacy Key Serialization
Legacy code stores `good_job_concurrency_key` in serialized_params. Need to ensure:

```ruby
# Keep existing serialization for backward compatibility
def serialize(*)
  super.tap do |job_data|
    job_data['good_job_concurrency_key'] = good_job_concurrency_key if good_job_concurrency_key.present?
    job_data['good_job_concurrency_labels'] = good_job_concurrency_labels if good_job_concurrency_labels.present?
  end
end

# On deserialization, legacy key becomes label
def deserialize(job_data)
  super
  self.good_job_concurrency_key = job_data['good_job_concurrency_key']
  
  # If there's a legacy key, it should be added to labels
  # (or handled by _good_job_concurrency_labels if legacy rule is present)
  self.good_job_concurrency_labels = job_data['good_job_concurrency_labels'] || []
end
```

### 2. Logging Message Consistency
Both systems log when constraints are exceeded. Unified logging:

```ruby
def _check_enqueue_rule_concurrency(rule)
  # ... existing logic ...
  
  if (enqueue_concurrency + 1) > limit
    constraint_type = rule.legacy? ? "concurrency key" : "concurrency label"
    logger.info "Aborted enqueue of #{self.class.name} (Job ID: #{job_id}) " \
                "because the #{constraint_type} '#{label}' has reached its " \
                "enqueue limit of #{limit} #{'job'.pluralize(limit)}"
    exceeded = :limit
  end
end
```

### 3. Migration Path for Users
- Old code: `good_job_control_concurrency_with(key: "...", total_limit: 5)` still works
- Internally converted to LegacyRule
- Users can gradually migrate to new syntax without breaking changes

---

## Phase 6: Testing Strategy

### Existing Tests Still Pass
- All legacy constraint tests should pass unchanged
- No new behavior, just different implementation

### New Tests
- Verify legacy and rules can coexist
- Verify legacy rule takes priority (unshift vs append)
- Verify label generation from legacy key
- Verify query behavior differs between legacy/rules

### Test Coverage
```ruby
describe "unified constraint checking" do
  it "legacy rule can coexist with new rules"
  it "legacy rule is checked first if both exist"
  it "legacy rule label comes from concurrency_key"
  it "legacy rule uses concurrency_key column queries"
  it "new rules use labels column queries"
  it "mixed legacy + new constraints work together"
end
```

---

## Benefits of This Refactoring

### Code Reduction
- **~110 lines** of duplicated constraint checking logic eliminated
- Callback logic reduced from ~190 lines to ~35 lines
- Single source of truth for checking logic

### Maintainability
- Add new constraint type → update Rule class
- Change checking algorithm → update `_check_enqueue_rule_concurrency` once
- Easier to debug (single code path)

### Consistency
- Both systems now guaranteed to behave identically
- Same logging, same locking, same transaction behavior
- Same error handling

### Future Extensibility
- New constraint types: create new Rule subclass
- New query strategies: override `query_scope`
- New limit types: override limit evaluation

### Backward Compatibility
- 100% compatible with existing code
- Users don't need to change anything
- Can migrate gradually or not at all

---

## Implementation Breakdown

| Phase | Changes | Files | Complexity |
|-------|---------|-------|-----------|
| 1 | Create LegacyRule class | rule.rb | Low |
| 2 | Update good_job_control_concurrency_with | concurrency.rb | Low |
| 3 | Add query_scope to Rule, update checkers | concurrency.rb, rule.rb | Medium |
| 4 | Remove legacy callback code | concurrency.rb | Medium |
| 5 | Handle serialization/edge cases | concurrency.rb | Low |
| 6 | Add tests for unified system | concurrency_spec.rb | Medium |

**Total Impact**:
- ~110 lines deleted
- ~40 lines added (LegacyRule class)
- Net reduction: ~70 lines
- Complexity reduced: ~30%

---

## Potential Issues & Mitigations

| Issue | Cause | Mitigation |
|-------|-------|-----------|
| Legacy key not becoming label | Deserialization order | Ensure LegacyRule label() handles key properly |
| Query fails for legacy rules | Wrong column queried | test_scope() method on Rule with proper override |
| Logging changes | Different message format | Keep similar formatting, just parameterize constraint type |
| Performance regression | Additional rule processing | Benchmark; no new DB queries, just iteration |
| Mixing legacy+new breaks | Rule order issues | Test mixed scenarios, ensure legacy checked first |

---

## Rollback Plan

If issues arise during implementation:

1. **Early phase issues**: Keep `good_job_concurrency_config` separate, don't integrate yet
2. **Late phase issues**: Keep legacy callback code, run both systems in parallel
3. **Testing issues**: Keep original tests, gradually migrate to new test patterns

The unified system can be feature-flagged if needed:

```ruby
if USE_UNIFIED_CHECKING
  rule = LegacyRule.new(config)
  self.good_job_concurrency_rules = good_job_concurrency_rules.unshift(rule)
else
  self.good_job_concurrency_config = config  # Old way
end
```

---

## Summary

**Goal**: Eliminate duplication by converting legacy system to use the same infrastructure as rules.

**Method**: Create `LegacyRule` adapter that makes legacy constraints look like a rule to the system.

**Result**: Single code path, single set of tests, 70% less code in callbacks.

**Backward Compatibility**: 100% - no breaking changes.

**Difficulty**: Medium - mostly plumbing, no new concepts.
