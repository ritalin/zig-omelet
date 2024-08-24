select id, status, $vis::Visibility
from Response
where status = $s::Status