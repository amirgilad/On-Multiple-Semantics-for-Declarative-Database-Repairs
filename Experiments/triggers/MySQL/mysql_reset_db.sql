delete from organization;
delete from author;
delete from writes;
delete from publication;
delete from cite;

delete from delta_organization;
delete from delta_author;
delete from delta_writes;
delete from delta_publication;
delete from delta_cite;

LOAD DATA LOCAL INFILE 'C:/Users/user/git/causal-rules/database_generator/author.csv'
INTO TABLE author FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE 'C:/Users/user/git/causal-rules/database_generator/writes.csv'
INTO TABLE writes FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE 'C:/Users/user/git/causal-rules/database_generator/publication.csv'
INTO TABLE publication FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE 'C:/Users/user/git/causal-rules/database_generator/cite.csv'
INTO TABLE cite FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';