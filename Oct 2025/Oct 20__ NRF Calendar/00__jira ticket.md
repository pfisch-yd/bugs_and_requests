# Jira Ticket

## link
(ENG-2404)[https://yipitdata5.atlassian.net/browse/ENG-2404]

@mnishiyama @pfisch - seeing differences in the nrf calendar tables for Target. As you can see, the date increment for target is a month behind the one for lowes (correct). We use the date as a reference to what calendar month we are referring to, so this is throwing off labels. Daniel had wanted to go over this with them today I believe so if we can run the latest changes that would be great!

code

%sql SELECT DISTINCT date, month_start, month_end, quarter, month FROM yd_sensitive_corporate.ydx_lowes_analysts_gold.lowes_v38_market_share_for_column_null_nrf_calendar;

| date       | month_start | month_end  | quarter | month |
|-------------|-------------|------------|----------|--------|
| 2025-09-01  | 2025-08-31  | 2025-10-04 | 3        | 8      |
| 2025-08-01  | 2025-08-03  | 2025-08-30 | 3        | 7      |
| 2025-07-01  | 2025-07-06  | 2025-08-02 | 2        | 6      |
| 2025-06-01  | 2025-06-01  | 2025-07-05 | 2        | 5      |



code
%sql SELECT DISTINCT date, month_start, month_end, quarter, month FROM yd_sensitive_corporate.ydx_target_analysts_gold.target_home_v38_market_share_for_column_null_nrf_calendar;

| date       | month_start | month_end  | quarter | month |
|-------------|-------------|------------|----------|--------|
| 2025-08-01  | 2025-08-31  | 2025-10-04 | 3        | 8      |
| 2025-07-01  | 2025-08-03  | 2025-08-30 | 3        | 7      |
| 2025-06-01  | 2025-07-06  | 2025-08-02 | 2        | 6      |
| 2025-05-01  | 2025-06-01  | 2025-07-05 | 2        | 5      |
