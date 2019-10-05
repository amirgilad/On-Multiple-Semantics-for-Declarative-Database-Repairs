CREATE TABLE organization (aid int, name varchar(60), oid int);
CREATE TABLE author (aid int, name varchar(60), oid int);
CREATE TABLE publication (pid int, title varchar(200), year int);
CREATE TABLE writes (aid int, pid int);
CREATE TABLE cite (citing int, cited int);


CREATE TABLE Delta_author (aid int, name varchar(60), oid int);
CREATE TABLE Delta_publication (pid int, title varchar(200), year int);
CREATE TABLE Delta_writes (aid int, pid int);
CREATE TABLE Delta_cite (citing int, cited int);
CREATE TABLE Delta_organization (oid int, name varchar(150));