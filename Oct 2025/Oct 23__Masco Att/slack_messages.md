# slack

(slack)[https://yipitdata.slack.com/archives/C06QMHHAQE8/p1761187663104939]

[diego]

Hey guys - noticing this issue with differences in attributes in the portal versus the raw table.
in the DB screenshot you'll notice that there is no attribute called 'Wall Plates' or 'Cabinet Hardware' or 'Bathroom Hardware' and what seems to be happening is that any attribute with a null value gets the L1 passed through as that attribute value. Not sure if y'all have seen this happen but it is a bit confusing to our clients since these L1s should not fall under attributes.

%sqL
select shower_door_type, count(*)
from ydx_masco_analysts_silver.masco_v38_filter_items
group by all

| shower_door_type                            | count(*)   |
| ------------------------------------------- | ---------- |
| null                                        | 12,392,118 |
| Shower screen / Glass panel / Splash screen | 853        |
| Pivot or Hinged                             | 19,474     |
| Neo-angle / Corner Unit                     | 185        |
| Sliding Tub                                 | 10,531     |
| Sliding Shower                              | 48,539     |
| Others                                      | 8,802      |


[juan]
In the changes currently in staging, this issue no longer appears, it looks like this now:
We’re planning to deploy this to production tonight, and I’ll let you know as soon as it’s live.
It would also be helpful if you could confirm that the attributes are displaying correctly for you in the image above.

[diego]

hey juan, just fyi i actually also adjusted the attributes so now instead of null it says N/A in my table, i just pushed that update this morning after i saw that issue yesterday night

in the underlying table i basically did coalesce(attribute, 'N/A') to fix this issue on my end

coalesce(attribute, 'N/A') to fix this issue on my end


[Juan Jaramillo  Today at 12:58 PM]
oh, that makes sense, so now you're correctly seeing the graph in the portal, right?


[Diego Mendoza  Today at 12:59 PM]
yep exactly! the graph is right in the portal now
but still probably needs a fix to deal with nulls for other clients
in case there are people on the team that do not coalesce


[Juan Jaramillo  Today at 12:59 PM]
awesome! I think we should replicate that logic for all the accounts upstream, what do you think @pfisch?


[Daniel Berner  Today at 1:00 PM]
If something can be fixed upstream, we should fix it upstream! Logic on the UI is the enemy.
