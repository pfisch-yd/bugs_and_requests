# Freeport

How are we working on

1. We create all tables in the normal database + catalog. For example : Triple Crown. We will update all tables in ydx_triplecrown_analysts_gold.

2. After creating all of the tables, we will freeport them. That is: (I will try to parallelize the process) we will freeport_geo() .. and freeport_market_share.

3. After freeporting them, if there is no changes in schema, you'll find the tables in
yd_fp_corporate_staging.{prod_schema}.{demo_name}_geographic_analysis__dmv__000
===> I want to consolidate the schema.
===> What will trigger a version update??

4. 


## RULES

1 - Add new columns : Free
2 - Change data type : PRICE, add version
3 - 

....

Other ideas:

1 - let the FP module schema free, that is, make it select * from
For that, I will need to concrete the blueprint for that module_table
(make sure no column has ALL caps. Make sure there are no changes on the datatype ...)