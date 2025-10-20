with
  Q1 as (
    select
      *
    from
      yd_sensitive_corporate.ydx_thd_analysts_gold.brand_view_dash_v3 brand_view_dash_v3
    where
      (merchant in ('Home Depot', 'Walmart'))
      and (department = 'D23F WALL/FLOOR COVERING')
      and (sub_class = 'SC09 CARPET TILE')
      and (class = 'C02 STOCK CARPET/MODULAR/UTI')
      and (clean_brand in ('TRAFFICMASTER', 'FOSS FLOORS'))
  ),
  Q15 as (
    select
      *
    from
      (
        select
          clean_brand_46 clean_brand_47,
          sum(rolling_merchant_gmv_48) SUM_57
        from
          (
            select
              clean_brand clean_brand_46,
              rolling_merchant_gmv rolling_merchant_gmv_48
            from
              Q1 Q12
          ) Q13
        group by
          clean_brand_46
      ) Q14
    where
      SUM_57 is not null
  ),
  Q29 as (
    select
      Q3.quarter quarter,
      Q3.rolling_merchant_gmv rolling_merchant_gmv,
      if (
        position(Q3.clean_brand in Q9.LISTAGGDISTINCT_105) > 0,
        Q3.clean_brand,
        'Other Brands'
      ) IF_107
    from
      (
        select
          Q2.quarter quarter,
          Q2.department department,
          Q2.class class,
          Q2.sub_class sub_class,
          Q2.channel channel,
          Q2.merchant merchant,
          Q2.clean_brand clean_brand,
          Q2.rolling_merchant_gmv rolling_merchant_gmv,
          Q7.quarter_16 quarter_16,
          Q7.RANK_19 RANK_19
        from
          Q1 Q2
          left join (
            select
              quarter_16,
              Rank() over (
                order by
                  quarter_16 desc
              ) RANK_19
            from
              (
                select
                  quarter_15 quarter_16
                from
                  (
                    select
                      quarter quarter_15
                    from
                      Q1 Q4
                  ) Q5
                group by
                  quarter_15
              ) Q6
          ) Q7 on Q2.quarter = Q7.quarter_16
        where
          (17 >= Q7.RANK_19)
          and (Q2.channel = 'Online')
          and (Q2.quarter in ('2025 1Q', '2024 4Q', '2024 3Q'))
      ) Q3
      cross join (
        select
          Q26.LISTAGGDISTINCT_104 LISTAGGDISTINCT_105
        from
          (
            select
              1 const_1
          ) totals
          cross join (
            select
              array_join(
                array_compact (
                  transform
                    (
                      array_sort(
                        array_agg (
                          distinct struct (clean_brand_47 as s0, clean_brand_47 as target)
                        ),
                        (l, r) -> (
                          case
                            when (l) ['s0'] is null
                            and (r) ['s0'] is null then 0
                            when (l) ['s0'] is null then -1
                            when (r) ['s0'] is null then 1
                            when (l) ['s0'] < (r) ['s0'] then -1
                            when (l) ['s0'] > (r) ['s0'] then 1
                            else 0
                          end
                        )
                      ),
                      st -> (st) ['target']
                    )
                ),
                ', '
              ) LISTAGGDISTINCT_104
            from
              (
                select
                  clean_brand_46 clean_brand_47
                from
                  (
                    select
                      *
                    from
                      (
                        select
                          clean_brand clean_brand_46
                        from
                          Q1 Q10
                      ) Q11
                    where
                      exists (
                        select
                          1
                        from
                          (
                            select
                              clean_brand_47 clean_brand_48
                            from
                              Q15 Q16
                          ) Q17
                        where
                          (
                            coalesce(Q11.clean_brand_46, '') = coalesce(Q17.clean_brand_48, '')
                          )
                          and (
                            (Q11.clean_brand_46 is not null) = (Q17.clean_brand_48 is not null)
                          )
                      )
                  ) Q19
                where
                  exists (
                    select
                      1
                    from
                      (
                        select
                          clean_brand_47 clean_brand_48
                        from
                          (
                            select
                              clean_brand_47,
                              SUM_57,
                              Rank() over (
                                order by
                                  IF_62 desc
                              ) RANK_63
                            from
                              (
                                select
                                  clean_brand_47,
                                  SUM_57,
                                  if (SUM_57 is not null, SUM_57, null) IF_62
                                from
                                  Q15 Q20
                              ) Q21
                          ) Q22
                        where
                          if (SUM_57 is not null, RANK_63 <= 15, null)
                      ) Q23
                    where
                      (
                        coalesce(Q19.clean_brand_46, '') = coalesce(Q23.clean_brand_48, '')
                      )
                      and (
                        (Q19.clean_brand_46 is not null) = (Q23.clean_brand_48 is not null)
                      )
                  )
              ) Q25
          ) Q26
      ) Q9
  )
select
  IF_108 Brand_Name,
  SUM_129 `sort-c3He2yXaNj-0`,
  quarter_111 Quarter,
  DIV_120 `%_of_Total_for_Sum_of_Rolling_Merchant_Gmv__by_X_Axis_`
from
  (
    select
      Q35.IF_108 IF_108,
      Q35.quarter_111 quarter_111,
      Q35.DIV_120 DIV_120,
      Q37.SUM_129 SUM_129,
      Q37.IF_108 IF_109
    from
      (
        select
          Q31.IF_108 IF_108,
          Q31.quarter_111 quarter_111,
          Q31.SUM_112 / try_cast (nullif(Q33.SUM_118, 0) as double) DIV_120
        from
          (
            select
              IF_107 IF_108,
              quarter quarter_111,
              sum(rolling_merchant_gmv) SUM_112
            from
              Q29 Q30
            group by
              IF_107,
              quarter
          ) Q31
          left join (
            select
              sum(rolling_merchant_gmv) SUM_118,
              quarter quarter_119
            from
              Q29 Q32
            group by
              quarter
          ) Q33 on (
            coalesce(Q31.quarter_111, '') = coalesce(Q33.quarter_119, '')
          )
          and (
            (Q31.quarter_111 is not null) = (Q33.quarter_119 is not null)
          )
      ) Q35
      left join (
        select
          sum(rolling_merchant_gmv) SUM_129,
          IF_107 IF_108
        from
          Q29 Q36
        group by
          IF_107
      ) Q37 on (coalesce(Q35.IF_108, '') = coalesce(Q37.IF_108, ''))
      and ((Q35.IF_108 is not null) = (Q37.IF_108 is not null))
  ) Q39
order by
  SUM_129 asc,
  IF_108 asc,
  quarter_111 asc
limit
  25001
  -- Sigma Σ {"sourceUrl":"https://app.sigmacomputing.com/yipit/workbook/Home-Depot-Merchant-Access-greater-than-greater-than-Oct1st-1o8XXhApQiLPhs78JjbZKY?:displayNodeId=mh6xzugmkX","kind":"adhoc","request-id":"g0199a11ed9fe77a18b94ecda156be75c","email":"pfisch@yipitdata.com"}