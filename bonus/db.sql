CREATE DATABASE IF NOT EXISTS elstat CHARACTER SET utf8mb4;
USE elstat;
CREATE TABLE records (
    id INT NOT NULL,
    country VARCHAR(30) NOT NULL,
    tourists INT NOT NULL,
    percentage DOUBLE NOT NULL,
    airplane INT NOT NULL,
    train INT NOT NULL,
    ship INT NOT NULL,
    car INT NOT NULL,
    month VARCHAR(7) NOT NULL,
    year VARCHAR(5) NOT NULL,
    PRIMARY KEY (id)
)ENGINE=INNODB;