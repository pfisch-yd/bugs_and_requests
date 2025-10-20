select
  gmv Gmv
from
  (
    /*
    Top SKUs -pf
     */
    with
      source as (
        --- basic table with filters
        SELECT
          *
        FROM
          yd_sensitive_corporate.ydx_prospect_analysts_gold.beauty_product_v38_sku_analysis
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
              else merchant in ('Ulta Beauty', 'Sephora')
            end
          )
          and (
            case
              when length(array_join(array(null), ',')) = 0 then true
              else channel in (null)
            end
          )
          and display_interval = 'Last Month'
      ),
      grouped as (
        select
          merchant,
          brand,
          web_description,
          product_id,
          min(major_cat) as major_category,
          min(sub_cat) as sub_category,
          min(minor_cat) as minor_category,
          sum(gmv) as total_gmv,
          sum(last_obs) as sum_last_obs,
          sum(last_gmv) as last_year_gmv,
          sum(total_units) as units,
          try_divide(sum(total_spend), sum(total_units)) as asp,
          Sum(Observations) as sample_size,
          min(product_hash) as product_hash
        from
          source
        group by
          merchant,
          brand,
          web_description,
          product_id
      )
    select
      sum(total_gmv) as gmv
    from
      grouped as add_columns
  ) Q1
limit
  10001
  -- Sigma Σ {"sourceUrl":"https://app.sigmacomputing.com/yipit/workbook/Brands-Master-V3-8-greater-than-Sept-15-beauty-issue-3hFk5JmhOzHNC7VEFrwPLC?:displayNodeId=2txEKwE98W","kind":"adhoc","request-id":"g01994e1ec47477dfb66cfda38fefb121","email":"pfisch@yipitdata.com"}