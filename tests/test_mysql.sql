-- for tests/test_sqlstore.py
CREATE DATABASE IF NOT EXISTS `test_sqlstore1`;

use `test_sqlstore1`;
DROP TABLE IF EXISTS `test_table1`;
CREATE TABLE `test_table1` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `name` varchar(10) NOT NULL,
    PRIMARY KEY (`id`)
) DEFAULT CHARSET=latin1; 

CREATE DATABASE IF NOT EXISTS `test_sqlstore2`;

use `test_sqlstore2`;
DROP TABLE IF EXISTS `test_table2`;
CREATE TABLE `test_table2` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `name` varchar(10) NOT null,
    PRIMARY KEY (`id`)
) DEFAULT CHARSET=latin1; 

CREATE DATABASE IF NOT EXISTS `test_sqlstore3`;

use `test_sqlstore3`;
DROP TABLE IF EXISTS `test_table3`;
CREATE TABLE `test_table3` (
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `name` varchar(10) NOT null,
    PRIMARY KEY (`id`)
) DEFAULT CHARSET=latin1;

GRANT SELECT, CREATE, UPDATE, DELETE, DROP ON `test_sqlstore1`.* TO 'sqlstore'@'127.0.0.1' IDENTIFIED BY 'sqlstore';
GRANT SELECT, CREATE, UPDATE, DELETE, DROP ON `test_sqlstore2`.* TO 'sqlstore'@'127.0.0.1' IDENTIFIED BY 'sqlstore';
GRANT SELECT, CREATE, UPDATE, DELETE, DROP ON `test_sqlstore3`.* TO 'sqlstore'@'127.0.0.1' IDENTIFIED BY 'sqlstore';
