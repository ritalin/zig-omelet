CREATE TABLE Temperature (
    id int primary key, 
    y int not null, 
    month_of_y int not null, 
    record_at DATE not null, 
    temperature FLOAT not null
)