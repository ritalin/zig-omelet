CREATE TABLE Response (
    id int primary key,
    data text,
    status ENUM('failed','success')
)