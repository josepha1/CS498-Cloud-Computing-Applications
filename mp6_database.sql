CREATE database mp6_db1;
USE mp6_db1;
CREATE TABLE mp6_table (
    id INT UNSIGNED AUTO_INCREMENT,
    hero VARCHAR(30),
    power VARCHAR(30),
    name VARCHAR(30),
    xp INT,
    color VARCHAR(30),
    PRIMARY KEY(id);
)