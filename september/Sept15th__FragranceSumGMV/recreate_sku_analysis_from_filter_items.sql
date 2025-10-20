select
  gmv Gmv
from
  (
    /*
    Recreate SKU Analysis from _filter_items without web_description quality filter
    Based on product_analysis.py logic but sourcing from _filter_items directly
     */
    with
      max_month as (
        SELECT month(max(month)) as max_month, MAX(month) as max_date
        FROM yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_filter_items
      ),

      source as (
        --- basic table with filters, handling null web_description
        SELECT
          *,
          coalesce(web_description, 'no_web_description') as web_description_clean
        FROM
          yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_filter_items
        CROSS JOIN max_month
        where
          (
            case
              when length(array_join(array(null), ',')) = 0 then true
              else brand in (null)
            end
          )
          and (
            case
              when length(array_join(array('fragrance'), ',')) = 0 then true
              else major_cat in ('fragrance')
            end
          )
          and (
            case
              when length(array_join(array(null), ',')) = 0 then true
              else sub_cat in (null)
            end
          )
          and (
            case
              when length(array_join(array(null), ',')) = 0 then true
              else minor_cat in (null)
            end
          )
          and (
            case
              when length(array_join(array('Ulta Beauty', 'Sephora'), ',')) = 0 then true
              else merchant_clean in ('Ulta Beauty', 'Sephora')
            end
          )
          and (
            case
              when length(array_join(array(null), ',')) = 0 then true
              else channel in (null)
            end
          )
          -- Filter for Last Month equivalent
          and date_trunc('month', month) = date_trunc('month', max_date)
      ),

      grouped as (
        select
          merchant_clean as merchant,
          brand,
          web_description_clean as web_description,
          coalesce(product_id, 'no_product_id') as product_id,
          min(major_cat) as major_category,
          min(sub_cat) as sub_category,
          min(minor_cat) as minor_category,
          sum(gmv) as total_gmv,
          -- Simulate last_obs and last_gmv (would need historical data for proper calculation)
          0 as sum_last_obs,
          0 as last_year_gmv,
          sum(item_quantity) as units,
          try_divide(sum(item_price * item_quantity), sum(item_quantity)) as asp,
          count(*) as sample_size,
          min(coalesce(product_hash, 'no_product_hash')) as product_hash
        from
          source
        group by
          merchant_clean,
          brand,
          web_description_clean,
          coalesce(product_id, 'no_product_id')
      )
    select
      sum(total_gmv) as gmv
    from
      grouped as add_columns
  ) Q1
limit
  10001
  -- Created to test GMV difference between market_share and sku_analysis approaches