with
    v as materialized (
        select Foo.id, Bar.id, xyz, kind, a from Foo
        join Bar on Foo.id = Bar.id
        cross join (
            select $a::int as a
        )
    ),
    v2 as materialized (
        select id, id_1 from v
    )
select id_1, id from v2