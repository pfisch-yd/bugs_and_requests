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
  ),
  Q3 as (
    select
      Q2.quarter quarter,
      Q2.gmv gmv,
      Q18.RANK_70 RANK_70,
      if (
        Q13.merchant_17 is null,
        'Other Retailers',
        Q13.merchant_17
      ) IF_93
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
      (Q2.channel = 'In-Store')
      and (
        Q2.quarter in ('2025 1Q', '2024 4Q', '2024 3Q', '2024 2Q')
      )
      and (Q2.class = 'C01 LIGHT BULBS')
      and (17 >= Q18.RANK_70)
  )
select
  IF_94 Retailer_Name,
  SUM_118 `sort-1W13AHrtVU-0`,
  quarter_97 Quarter,
  DIV_108 `%_R4Q_Market_Share`
from
  (
    select
      Q27.IF_94 IF_94,
      Q27.quarter_97 quarter_97,
      Q27.DIV_108 DIV_108,
      Q30.SUM_118 SUM_118,
      Q30.IF_94 IF_95
    from
      (
        select
          Q22.IF_94 IF_94,
          Q22.quarter_97 quarter_97,
          Q22.SUM_99 / try_cast (nullif(Q25.SUM_106, 0) as double) DIV_108
        from
          (
            select
              IF_93 IF_94,
              quarter quarter_97,
              sum(gmv) SUM_99
            from
              (
                select
                  *
                from
                  Q3 Q20
                where
                  17 >= RANK_70
              ) Q21
            group by
              IF_93,
              quarter
          ) Q22
          left join (
            select
              sum(gmv) SUM_106,
              quarter quarter_107
            from
              (
                select
                  *
                from
                  Q3 Q23
                where
                  17 >= RANK_70
              ) Q24
            group by
              quarter
          ) Q25 on (
            coalesce(Q22.quarter_97, '') = coalesce(Q25.quarter_107, '')
          )
          and (
            (Q22.quarter_97 is not null) = (Q25.quarter_107 is not null)
          )
      ) Q27
      left join (
        select
          sum(gmv) SUM_118,
          IF_93 IF_94
        from
          (
            select
              *
            from
              Q3 Q28
            where
              17 >= RANK_70
          ) Q29
        group by
          IF_93
      ) Q30 on (coalesce(Q27.IF_94, '') = coalesce(Q30.IF_94, ''))
      and ((Q27.IF_94 is not null) = (Q30.IF_94 is not null))
  ) Q32
order by
  SUM_118 asc,
  IF_94 asc,
  quarter_97 asc
limit
  25001
  -- Sigma Σ {"sourceUrl":"https://app.sigmacomputing.com/yipit/workbook/Home-Depot-Merchant-Access-Sept17th-6wN71SWeH7GwojZmbmZdEj?:displayNodeId=3XmyNdMAuL","kind":"adhoc","request-id":"g01999fc40c6575cd986cabe9c12a61c5","email":"pfisch@yipitdata.com"}