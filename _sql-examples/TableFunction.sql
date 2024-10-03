select 
    unnest(data_1, recursive := true), unnest(data_2)
from (
    select unnest(j)
    from read_json("$dataset" := '_dataset-examples/struct_sample_2.json') t(j)
)
