INSERT INTO Foo (id, kind, xyz)
VALUES ($id, $kind, $xyz)
ON CONFLICT (id)
DO UPDATE
SET xyz = EXCLUDED.xyz * $n::int
WHERE xyz is not null and xyz % $div::int = $mod