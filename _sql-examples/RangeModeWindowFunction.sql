select y, month_of_y, record_at, 
    avg(temperature) 
    over (
        partition by y, month_of_y
        order by record_at
        range between interval ($days::int) days preceding and current row
    ) as a
from Temperature