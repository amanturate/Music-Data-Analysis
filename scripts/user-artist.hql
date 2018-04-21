CREATE DATABASE IF NOT EXISTS project;

USE project;

CREATE TABLE 
(
user_id STRING,
artists_array ARRAY<STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '&';

LOAD DATA LOCAL INPATH '/home/acadgild/project/lookupfiles/user-artist.txt'
OVERWRITE INTO TABLE users_artists;



INSERT OVERWRITE LOCAL DIRECTORY '/home/acadgild/project/user_artists/'

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','

STORED AS TEXTFILE

SELECT user_id, artists FROM users_artists LATERAL VIEW explode(artists_array) a AS artists;	
