## Pending funded

group = not part of funded groups
funded_at = not null

phase = 1
status = 2
fund_status = 0

## Approved funded

group = must be part of funded groups
funded_at = not null
is_active = 1

phase = 0
status = 1
fund_status = 1

## Rejected funded

group = not part of funded groups
funded_at = not null
is_active = 0

phase = 1
status = 0
fund_status = 2

## List of Funded Groups

demo\Nostro\U-FTF-1-B
demo\Nostro\U-COF-1-B
demo\Nostro\U-SSF-1-B
demo\Nostro\U-SAF-1-B
demo\Nostro\U-DSF-1-B
demo\Nostro\U-DAF-1-B
demo\Nostro\U-TSF-1-B
demo\Nostro\U-TAF-1-B

For pending funded account (Last Phase Group)
1-B
2-B
3-B

For evolution Phase

'singleStandard' => 'demo\Nostro\U-SST-1-B',
'doubleStandard' => 'demo\Nostro\U-DST-1-B',
phase = 1,2,3 funded_status = 0 ,status = is_active
