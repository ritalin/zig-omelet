select id, vis, 'success'::ENUM('failed','success')
from Control
where vis = $vis::ENUM('hide','visible')
