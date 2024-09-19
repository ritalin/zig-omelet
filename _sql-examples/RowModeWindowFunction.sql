select id, 
    sum($val::int) 
    over (
        rows between $from_row preceding and $to_row following
    ) as a
from Foo