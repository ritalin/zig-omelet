select $id::bigint, $name::varchar from foo where kind = $kind::int;
select $2::text as name, xyz, 123 from Foo where kind = $1::int;
