with
  Q1 as (
    select
      *
    from
      yd_sensitive_corporate.ydx_thd_analysts_gold.sigma_competitor_share_v2 sigma_competitor_share_v2
    where
      department in (
        'D21 LUMBER',
        'D22 BUILDING MATERIALS',
        'D23F WALL/FLOOR COVERING',
        'D24 PAINT',
        'D25H HARDWARE',
        'D25P POWER',
        'D25S STORAGE AND ORGANIZATION',
        'D26P PLUMBING',
        'D27E ELECTRICAL',
        'D27L LIGHTING',
        'D28I INDOOR GARDEN',
        'D28O OUTDOOR GARDEN',
        'D29A APPLIANCES',
        'D29B BATH',
        'D29K KITCHEN AND BLINDS',
        'D30 MILLWORK',
        'D59H HOME'
      )
  )
select
  SUM_99 Sum_of_Gmv
from
  (
    select
      sum(gmv) SUM_99
    from
      (
        select
          Q2.quarter quarter,
          Q2.merchant merchant,
          Q2.channel channel,
          Q2.department department,
          Q2.class class,
          Q2.gmv gmv,
          Q13.merchant_17 merchant_17,
          Q13.IF_23 IF_23,
          Q13.RANK_29 RANK_29,
          Q18.quarter_67 quarter_67,
          Q18.RANK_70 RANK_70
        from
          Q1 Q2
          left join (
            select
              Q10.merchant_17 merchant_17,
              Q10.IF_23 IF_23,
              Q10.RANK_29 RANK_29
            from
              (
                select
                  *
                from
                  (
                    select
                      merchant_17,
                      IF_23,
                      Rank() over (
                        order by
                          IF_28 desc
                      ) RANK_29
                    from
                      (
                        select
                          merchant_17,
                          IF_23,
                          if (IF_23 is not null, IF_23, null) IF_28
                        from
                          (
                            select
                              merchant_17,
                              if (merchant_17 = 'Home Depot', SUM_21 * 10000000, SUM_21) IF_23
                            from
                              (
                                select
                                  merchant_16 merchant_17,
                                  sum(gmv_15) SUM_21
                                from
                                  (
                                    select
                                      gmv gmv_15,
                                      merchant merchant_16
                                    from
                                      Q1 Q4
                                  ) Q5
                                group by
                                  merchant_16
                              ) Q6
                          ) Q7
                        where
                          IF_23 is not null
                      ) Q8
                  ) Q9
                where
                  if (IF_23 is not null, RANK_29 <= 15, null)
              ) Q10
              cross join (
                select
                  1
              ) Q11
          ) Q13 on Q2.merchant = Q13.merchant_17
          left join (
            select
              quarter_67,
              Rank() over (
                order by
                  quarter_67 desc
              ) RANK_70
            from
              (
                select
                  quarter_66 quarter_67
                from
                  (
                    select
                      quarter quarter_66
                    from
                      Q1 Q15
                  ) Q16
                group by
                  quarter_66
              ) Q17
          ) Q18 on Q2.quarter = Q18.quarter_67
        where
          (
            (Q2.channel = 'In-Store')
            and (
              Q2.quarter in ('2025 1Q', '2024 4Q', '2024 3Q', '2024 2Q')
            )
            and (Q2.class = 'C01 LIGHT BULBS')
            and (17 >= Q18.RANK_70)
          )
          and (
            (Q2.class = 'C01 LIGHT BULBS')
            and (
              (lower(Q2.quarter) = lower('2025 2Q'))
              or (lower(Q2.quarter) is null)
            )
            and (Q2.channel = 'In-Store')
          )
      ) Q3
  ) Q20
  -- Sigma Σ {"sourceUrl":"https://app.sigmacomputing.com/yipit/workbook/Home-Depot-Merchant-Access-Sept17th-6wN71SWeH7GwojZmbmZdEj?:displayNodeId=dgz_s5zZA0","kind":"adhoc","request-id":"g01999fc40c6575b4ae1a84dc3a4e8711","email":"pfisch@yipitdata.com"}