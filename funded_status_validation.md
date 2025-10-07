## List of Funded Groups

demo\Nostro\U-FTF-1-A
demo\Nostro\U-FTF-1-B
demo\Nostro\U-COF-1-A
demo\Nostro\U-COF-1-B
demo\Nostro\U-SSF-1-A
demo\Nostro\U-SSF-1-B
demo\Nostro\U-SAF-1-A
demo\Nostro\U-SAF-1-B
demo\Nostro\U-DSF-1-A
demo\Nostro\U-DSF-1-B
demo\Nostro\U-DAF-1-A
demo\Nostro\U-DAF-1-B
demo\Nostro\U-TSF-1-A
demo\Nostro\U-TSF-1-B
demo\Nostro\U-TAF-1-A
demo\Nostro\U-TAF-1-B

## For checking current_phase Group check if group contain

1-A or 1-B then current_phase = 1
2-A or 2-B then current_phase = 2
3-A or 3-B then current_phase = 3

Scenario 1 when funded_at is not null

## Pending funded

group = not part of funded groups
funded_at = not null

current_phase = 1
status = 2
fund_status = 0

## Approved funded

group = must be part of funded groups
funded_at = not null
is_active = 1

current_phase = 0
status = 1
fund_status = 1

## Rejected funded

group = not part of funded groups
funded_at = not null
is_active = 0

current_phase = 1
status = 0
fund_status = 2

Scenario 2 when funded_at is null

For evolution Phase
current_phase = 1,2,3 (check above condition to get current phase)
funded_status = 0
status = is_active
