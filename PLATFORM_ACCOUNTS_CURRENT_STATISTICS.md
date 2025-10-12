# Platform Accounts Current Statistics Analysis

## Overview

This document provides a comprehensive analysis of the current platform accounts export results after git reset, focusing on the funded status distribution and explaining why most accounts show as "Pending Funded".

## Executive Summary

| Metric                  | Count  | Percentage |
| ----------------------- | ------ | ---------- |
| **Total Accounts**      | 86,294 | 100.00%    |
| **Pending Funded (0)**  | 85,460 | 99.03%     |
| **Approved Funded (1)** | 774    | 0.90%      |
| **Rejected Funded (2)** | 12     | 0.01%      |
| **Other (None)**        | 48     | 0.06%      |

## Detailed Analysis

### 1. Evolution vs Funded Phase Breakdown

| Phase Type          | Count  | Percentage | Description                                      |
| ------------------- | ------ | ---------- | ------------------------------------------------ |
| **Evolution Phase** | 85,274 | 98.82%     | Challenge phase accounts (funded_at is null)     |
| **Funded Phase**    | 1,020  | 1.18%      | Actually funded accounts (funded_at is not null) |

### 2. Current Phase Distribution

| Phase       | Count  | Percentage | Description                        |
| ----------- | ------ | ---------- | ---------------------------------- |
| **Phase 0** | 774    | 0.90%      | Funded accounts (approved)         |
| **Phase 1** | 79,570 | 92.21%     | Challenge phase 1 (1-A/1-B groups) |
| **Phase 2** | 5,026  | 5.82%      | Challenge phase 2 (2-A/2-B groups) |
| **Phase 3** | 924    | 1.07%      | Challenge phase 3 (3-A/3-B groups) |

### 3. Account Status Distribution

| Status           | Count  | Percentage | Description       |
| ---------------- | ------ | ---------- | ----------------- |
| **Inactive (0)** | 72,192 | 83.66%     | Inactive accounts |
| **Active (1)**   | 13,868 | 16.07%     | Active accounts   |
| **Pending (2)**  | 186    | 0.22%      | Pending status    |
| **Other (None)** | 48     | 0.06%      | Undefined status  |

### 4. Funded Accounts Detailed Analysis

**Total accounts with funded_at set: 1,020 (1.18%)**

| Funded Status           | Count | Percentage | Description                   |
| ----------------------- | ----- | ---------- | ----------------------------- |
| **Approved Funded (1)** | 774   | 75.88%     | Successfully funded accounts  |
| **Pending Funded (0)**  | 186   | 18.24%     | Funded but pending approval   |
| **Rejected Funded (2)** | 12    | 1.18%      | Rejected funding applications |
| **Other (None)**        | 48    | 4.71%      | Undefined status              |

### 5. Platform Groups Analysis

- **Total Platform Groups**: 32
- **Funded Groups**: 7

**Funded Groups List:**

1. `demo\Nostro\U-TAF-1-B`
2. `demo\Nostro\U-SAF-1-B`
3. `demo\Nostro\U-DSF-1-B`
4. `demo\Nostro\U-DAF-1-B`
5. `demo\Nostro\U-SSF-1-B`
6. `demo\Nostro\U-FTF-1-B`
7. `demo\Nostro\U-TSF-1-B`

## Key Insights

### ✅ **The High "Pending Funded" Percentage is CORRECT**

The 99.03% "Pending Funded" status is **expected and correct** because:

1. **Evolution Phase Dominance**: 98.82% of accounts are in the Evolution Phase (challenge phase) where `funded_at` is null
2. **Correct Logic Application**: Evolution Phase accounts correctly have `funded_status = 0` (Pending Funded)
3. **Low Funding Rate**: Only 1.18% of accounts have actually been funded (`funded_at` is not null)
4. **Reasonable Approval Rate**: Of the 1,020 funded accounts, 75.9% are Approved Funded

### 📊 **Business Logic Validation**

The statistics align with expected business patterns:

- **Challenge Phase**: Most users are in challenge phases (Phase 1-3) trying to qualify for funding
- **Low Funding Rate**: Only 1.18% of accounts reach the funding stage
- **High Approval Rate**: 75.9% of funded accounts are approved (good success rate)
- **Proper Phase Distribution**: 92.21% in Phase 1, 5.82% in Phase 2, 1.07% in Phase 3

### 🔍 **Funded Status Logic Breakdown**

1. **Evolution Phase (98.82%)**: `funded_at = null` → `funded_status = 0` (Pending Funded)
2. **Funded Phase (1.18%)**: `funded_at ≠ null` → Logic based on group membership and `is_active`:
   - **Approved Funded (75.9%)**: Group in funded groups + `is_active = 1`
   - **Pending Funded (18.2%)**: Group not in funded groups
   - **Rejected Funded (1.2%)**: Group not in funded groups + `is_active = 0`

## Conclusion

### ✅ **Current Results are CORRECT**

The platform accounts export is working correctly. The high percentage of "Pending Funded" accounts (99.03%) is the expected behavior because:

1. **Most accounts are in challenge phase** (Evolution Phase) where they haven't been funded yet
2. **Only 1.18% of accounts have been funded** (`funded_at` is set)
3. **Of those funded accounts, 75.9% are approved**, which is a reasonable success rate
4. **The funded status logic is applied correctly** according to the business rules

### 🎯 **No Issues Found**

There are no bugs or issues with the current implementation. The statistics reflect the natural distribution of accounts in a trading challenge platform where:

- Most users are in challenge phases
- Only a small percentage reach funding
- The majority of funded accounts are approved

### 📈 **Recommendations**

1. **Keep Current Implementation**: The script is working correctly
2. **Monitor Trends**: Track how the percentages change over time
3. **Business Metrics**: Use these statistics for business analysis and reporting

---

_Generated on: $(date)_
_Script: platform_accounts_export.py (after git reset)_
_Total Accounts: 86,294_
