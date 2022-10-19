SELECT *from movies;

UPDATE movies SET title ='xxxxx' WHERE movieid=1; -- befor update value : Toy Story (1995)

SELECT *FROM movies AT (OFFSET => -60*2);

SET timezone ='UTC';

SELECT CURRENT_timestamp(); --2022-10-17 15:55:30.890

SELECT *FROM movies BEFORE (timestamp => '2022-10-16 14:55:30.890'::timestamp);



select *
from table(information_schema.query_history_by_session())
order by start_time;


select *
from table(information_schema.query_history())
order by start_time;

SELECT *FROM movies BEFORE (STATEMENT => '01a7afad-3200-91d8-0001-f5ce0002e006');