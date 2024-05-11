# Qdrant fdw
This extension for postgresql allows you to make reading queries to the quadrant vector database. The following operations are currently supported:
1. read
2. order by push down (with restrictions)
3. limit/offset push down
4. some filters push down
5. columns mapping

todo:
1. [ ] multiply vector per row support
2. [ ] DML
3. [ ] analyze
4. [ ] recommend API
5. [ ] search parameters

## Restrictions
### ORDER BY
Currently, for a push down ORDER BY, it must meet the following requirements:
1. use the sim function with an unmodified vector.
2. have a limit.

At the moment, we cannot sort on the postgresql side because we do not know the similarity metric selected on the qdrant side.
For the same reason, the sim function is a "meta" function that always returns NULL.

### WHERE
if we want to push down an order by or limit, we must also pushdown the filter entirely, so if any of the filters cannot be pushdown, we cannot pushdown anything.

### Reserved attribute names
The three attribute names reserved are:
1. id - id of the record
2. vector - currently, only one vector per record is supported. That's what it is.
3. payload - raw payload.

All other attribute names are tread as field names in the payload. So far, only one json layer is supported.
Missing fields will have a null value. The null check occurs on the postgres side and may occur after the limit (it will most likely be changed in the future).
## Examples

```postgresql
create extension qdrant_fdw;

create server foreign_server
    foreign data wrapper qdrant_fdw
    options (host '127.0.0.1', port '6333');


create foreign table qdrant_table (
    id integer,
    vector varchar,
    payload varchar,
    str varchar,
    a float4,
    b int)
    SERVER foreign_server
    OPTIONS (collection 'test');

-- search

explain select * from qdrant_table order by sim(vector, '[0, 0, 0]') limit 3;
-- The request body in explain is most often not valid json.
-- Since I don't want to implement the arguments deparse by myself -_-
--                             QUERY PLAN                            
-- ------------------------------------------------------------------
--  Foreign Scan on qdrant_table  (cost=0.00..0.00 rows=0 width=116)
--    POST: http://127.0.0.1:6333/collections/test/points/search
--    : {
--          "vector":       '[0, 0, 0]'::character varying,
--          "limit":        '3'::bigint,
--          "with_vector":  true,
--          "with_payload": true
--  }
-- (8 rows)

select * from qdrant_table order by sim(vector, '[0, 0, 0]') limit 3;
--  id |              vector               |                        payload                        |     str     | a | b 
-- ----+-----------------------------------+-------------------------------------------------------+-------------+---+---
--   3 | [0.4558423,0.5698029,0.68376344]  | {"a":4,"load":4.5,"name":"test4"}                     |             | 4 |  
--   6 | [0.49153918,0.5734623,0.65538555] | {"a":5,"load":5.5,"name":"test5","str":"hello world"} | hello world | 5 |  
--   2 | [0.4242641,0.56568545,0.70710677] | {"a":3,"load":3.5,"name":"test3"}                     |             | 3 |  
-- (3 rows)

--filter

explain select * from qdrant_table where b = 20 order by sim(vector, '[0, 0, 0]') limit 3;
--                             QUERY PLAN                            
-- ------------------------------------------------------------------
--  Foreign Scan on qdrant_table  (cost=0.00..0.00 rows=0 width=116)
--    POST: http://127.0.0.1:6333/collections/test/points/search
--    : {
--          "vector":       '[0, 0, 0]'::character varying,
--          "limit":        '3'::bigint,
--          "filter":       {
--                  "must": [{
--                                  "key":  "b",
--                                  "match":        {
--                                          "value":        20
--                                  }
--                          }]
--          },
--          "with_vector":  true,
--          "with_payload": true
--  }
-- (16 rows)

select * from qdrant_table where b = 20 order by sim(vector, '[0, 0, 0]') limit 3;
--  id |              vector               |                 payload                  | str | a | b  
-- ----+-----------------------------------+------------------------------------------+-----+---+----
--   7 | [0.49153918,0.5734623,0.65538555] | {"a":5,"b":20,"load":5.5,"name":"test5"} |     | 5 | 20
-- (1 row)

```
