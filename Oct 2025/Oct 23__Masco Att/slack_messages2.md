# slack

(link)[https://yipitdata.slack.com/archives/C06QMHHAQE8/p1761228788604509]

[diego]
Also noticed that it looks like the attribute filters disappear on the Masco portal once I make a retailer selection; seems like brand and category selections do not depopulate the attribute filter like retailer does


Juan Jaramillo
  Yesterday at 2:30 PM
I understand the issue. In general, what’s happening is that the top-level attribute filters point to the _filter_items gold table, while the rest of the top-level filters point to the _market_share_for_column_null gold table — the same one used across the rest of the Share Breakdown module.
Because of this, there are occasional inconsistencies — like in the case of Amazon, where in _filter_items the merchant appears as “amazon_leia”, but in _market_share_for_column_null it appears as “amazon.”
 So, when filtering Attribute options by “amazon,” no results are returned.
2:34
To address this issue, I’m thinking of using the same query we use in PA for the top-level attribute filters, pointing to the _sku_time_series gold table.
This table is much more compatible with the one we use for the rest of Share Breakdown.
We can’t reuse the current SB gold table for these filters since it doesn’t currently support attribute data due to its aggregation level.
@pfisch, does that sound good to you if I implement this change?

Juan Jaramillo
  Yesterday at 3:28 PM
For now, a simpler and faster solution could be to make the top-level Attribute filter independent — meaning the values selected in the other top-level filters wouldn’t affect the options it displays.
What do you think, @dberner?


Daniel Berner
  Yesterday at 3:49 PM
I think we leave it as-is until we clean up/change which upstream tables we're querying
3:49
Lets do that as part of the react migration?


Juan Jaramillo
  Yesterday at 5:09 PM
I agree, we can handle that after we complete Price Bands Phase 2 and when we start implementing the new Attribute filter UI in React (where we want both attribute keys and values in a single select)
