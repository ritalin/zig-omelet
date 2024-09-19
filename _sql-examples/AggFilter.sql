select z, count(*) filter (where fmod(x, $div::int) > $rem::int) as c 
from "Point"
group by z