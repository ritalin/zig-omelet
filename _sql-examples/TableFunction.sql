select $name::varchar as name, xyz, 123 
from read_json($path::varchar) t(id, v, v2) 
where v = $value::int and v2 = $value2::bigint