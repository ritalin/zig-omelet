with recursive
    t(n, k) AS (
        VALUES (0, 42)
        UNION ALL
        SELECT n+1, k*2 FROM t WHERE n < $max_value::int
    ),
    t2(m, h) as (
        select n + $delta::int as m, k from t
        union all
        select m*2, h-1 from t2 where m < $max_value2::int
    )
SELECT h, m FROM t2 
