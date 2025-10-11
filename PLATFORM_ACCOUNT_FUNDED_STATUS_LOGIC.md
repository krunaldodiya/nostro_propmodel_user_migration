# Platform Account Funded Status Logic Documentation

## Overview

This document explains the complete logic used in the platform accounts export script to determine the `funded_status`, `current_phase`, and `status` fields for each platform account. The logic is based on two main scenarios depending on whether the `funded_at` field is null or not.

## Key Fields

- **`funded_at`**: Timestamp indicating when the account was funded (null for evolution phase accounts)
- **`group`**: The platform group the account belongs to
- **`is_active`**: Boolean flag indicating if the account is currently active
- **`funded_status`**: Final funding status (0=not funded, 1=approved/funded, 2=rejected)
- **`current_phase`**: Current trading phase (0=funded, 1=phase 1, 2=phase 2, 3=phase 3)
- **`status`**: Account status (0=inactive, 1=active, 2=pending)

## Funded Groups List

The following groups are considered "funded" groups in the system:

```
demo\Nostro\U-FTF-1-A    demo\Nostro\U-FTF-1-B
demo\Nostro\U-COF-1-A    demo\Nostro\U-COF-1-B
demo\Nostro\U-SSF-1-A    demo\Nostro\U-SSF-1-B
demo\Nostro\U-SAF-1-A    demo\Nostro\U-SAF-1-B
demo\Nostro\U-DSF-1-A    demo\Nostro\U-DSF-1-B
demo\Nostro\U-DAF-1-A    demo\Nostro\U-DAF-1-B
demo\Nostro\U-TSF-1-A    demo\Nostro\U-TSF-1-B
demo\Nostro\U-TAF-1-A    demo\Nostro\U-TAF-1-B
```

## Current Phase Logic

The `current_phase` is determined by the group name pattern:

| Group Pattern           | Current Phase | Description          |
| ----------------------- | ------------- | -------------------- |
| Contains `1-A` or `1-B` | 1             | Phase 1 (Evaluation) |
| Contains `2-A` or `2-B` | 2             | Phase 2 (Evaluation) |
| Contains `3-A` or `3-B` | 3             | Phase 3 (Evaluation) |
| Default                 | 1             | Fallback to Phase 1  |

## Scenario Logic

### Scenario 1: Funded Phase (`funded_at` is NOT null)

When an account has a `funded_at` timestamp, it means the account has gone through the funding process. The logic then determines the final status based on group membership and active status.

#### 1.1 Pending Funded Phase

**Conditions:**

- `funded_at` is NOT null
- `group` is NOT in the funded groups list

**Result:**

- `current_phase = 1, 2, or 3` (preserves original phase from group pattern)
- `status = 2` (pending)
- `funded_status = 0` (not funded)

**Example:**

```
Account with group "demo\Nostro\U-DST-2-B" and funded_at = "2024-01-15"
→ Pending Funded Phase (group not in funded list)
→ current_phase = 2 (from group pattern 2-B), status = 2, funded_status = 0
```

#### 1.2 Approved Funded Phase

**Conditions:**

- `funded_at` is NOT null
- `group` IS in the funded groups list
- `is_active = 1`

**Result:**

- `current_phase = 0` (funded phase)
- `status = 1` (active)
- `funded_status = 1` (approved/funded)

**Example:**

```
Account with group "demo\Nostro\U-FTF-1-A", funded_at = "2024-01-15", is_active = 1
→ Approved Funded Phase (group in funded list + active)
→ current_phase = 0, status = 1, funded_status = 1
```

#### 1.3 Rejected Funded Phase

**Conditions:**

- `funded_at` is NOT null
- `group` is NOT in the funded groups list
- `is_active = 0`

**Result:**

- `current_phase = 1, 2, or 3` (preserves original phase from group pattern)
- `status = 0` (inactive)
- `funded_status = 2` (rejected)

**Example:**

```
Account with group "demo\Nostro\U-SAG-3-B", funded_at = "2024-01-15", is_active = 0
→ Rejected Funded Phase (group not in funded list + inactive)
→ current_phase = 3 (from group pattern 3-B), status = 0, funded_status = 2
```

### Scenario 2: Evolution Phase (`funded_at` IS null)

When an account has a null `funded_at` timestamp, it means the account is still in the evaluation phase and hasn't been funded yet.

#### Evolution Phase

**Conditions:**

- `funded_at` IS null

**Result:**

- `current_phase = 1, 2, or 3` (based on group pattern)
- `funded_status = 0` (not funded)
- `status = is_active` (copies the is_active field value)

**Examples:**

```
Account with group "demo\Nostro\U-DAG-1-B", funded_at = null, is_active = 1
→ Evolution Phase (Phase 1)
→ current_phase = 1, status = 1, funded_status = 0

Account with group "demo\Nostro\U-SAG-2-A", funded_at = null, is_active = 0
→ Evolution Phase (Phase 2)
→ current_phase = 2, status = 0, funded_status = 0
```

## Decision Tree

```
Is funded_at null?
├── YES → Evolution Phase
│   ├── current_phase = 1/2/3 (based on group pattern)
│   ├── funded_status = 0
│   └── status = is_active
│
└── NO → Funded Phase
    ├── Is group in funded groups list?
    │   ├── YES → Is is_active = 1?
    │   │   ├── YES → Approved Funded Phase
    │   │   │   ├── current_phase = 0
    │   │   │   ├── status = 1
    │   │   │   └── funded_status = 1
    │   │   └── NO → (This case is not explicitly handled in current logic)
    │   └── NO → Is is_active = 0?
    │       ├── YES → Rejected Funded Phase
    │       │   ├── current_phase = 1/2/3 (preserve from group pattern)
    │       │   ├── status = 0
    │       │   └── funded_status = 2
    │       └── NO → Pending Funded Phase
    │           ├── current_phase = 1/2/3 (preserve from group pattern)
    │           ├── status = 2
    │           └── funded_status = 0
```

## Status Values Summary

### funded_status Values

- **0**: Not funded (Evolution Phase or Pending Funded Phase)
- **1**: Approved/Funded (Approved Funded Phase)
- **2**: Rejected (Rejected Funded Phase)

### current_phase Values

- **0**: Funded phase (only for approved funded accounts)
- **1**: Phase 1 (evaluation phase 1, or pending/rejected funded from 1-A/1-B groups)
- **2**: Phase 2 (evaluation phase 2, or pending/rejected funded from 2-A/2-B groups)
- **3**: Phase 3 (evaluation phase 3, or pending/rejected funded from 3-A/3-B groups)

### status Values

- **0**: Inactive
- **1**: Active
- **2**: Pending (for pending funded phase)

## Implementation Notes

1. **Priority Order**: The logic is applied in a specific order:

   - First: Scenario 2 (Evolution Phase) for all accounts with null `funded_at`
   - Then: Scenario 1.1 (Pending Funded Phase)
   - Then: Scenario 1.2 (Approved Funded Phase)
   - Finally: Scenario 1.3 (Rejected Funded Phase)

2. **Group Pattern Matching**: The current phase is determined by string pattern matching on the group name (1-A/1-B, 2-A/2-B, 3-A/3-B).

3. **Funded Groups**: The list of funded groups is hardcoded in the script and represents the groups that accounts must be in to be considered "approved funded".

4. **Edge Cases**:

   - Accounts with `funded_at` not null, group in funded list, but `is_active = 0` are not explicitly handled in the current logic
   - The logic assumes that if an account is in a funded group and has `funded_at` set, it should be active

5. **Current Implementation Bug**:
   - **⚠️ BUG**: The current script incorrectly sets `current_phase = 1` for both Pending Funded Phase and Rejected Funded Phase
   - **Should be**: Preserve the original `current_phase` from the group pattern (1, 2, or 3)
   - **Example**: Account with group `demo\Nostro\U-DST-2-B` should have `current_phase = 2`, not `current_phase = 1`
   - **Fix needed**: Update the script to use `pl.col("current_phase")` instead of `pl.lit(1)` for pending and rejected funded phases

## Testing Scenarios

To test the logic, you can use these example accounts:

```csv
# Evolution Phase (Phase 1)
login,group,funded_at,is_active,expected_current_phase,expected_status,expected_funded_status
123456,demo\Nostro\U-DAG-1-B,,1,1,1,0

# Evolution Phase (Phase 2)
123457,demo\Nostro\U-SAG-2-A,,0,2,0,0

# Approved Funded Phase
123458,demo\Nostro\U-FTF-1-A,2024-01-15,1,0,1,1

# Pending Funded Phase
123459,demo\Nostro\U-DAG-1-B,2024-01-15,1,1,2,0

# Rejected Funded Phase
123460,demo\Nostro\U-DAG-1-B,2024-01-15,0,1,0,2
```

---

_This documentation was created to explain the funded status logic implemented in `scripts/platform_accounts_export.py`_
