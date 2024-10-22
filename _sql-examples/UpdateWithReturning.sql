update Foo 
set xyz = $v1, remarks = $remarks
where kind = $kind
returning id
