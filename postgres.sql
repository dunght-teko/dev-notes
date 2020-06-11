-- ####################
-- abort/kill running query
SELECT pg_cancel_backend(procpid);
SELECT pg_terminate_backend(procpid);


-- ####################
-- check blocked queries
SELECT
  blocked_locks.pid         AS blocked_pid,
	blocked_activity.usename  AS blocked_user,
	blocking_locks.pid        AS blocking_pid,
  blocking_activity.usename AS blocking_user,
	blocked_activity.query    AS blocked_statement,
	blocking_activity.query   AS current_statement_in_blocking_process
FROM pg_catalog.pg_locks  blocked_locks
JOIN pg_catalog.pg_stat_activity  blocked_activity
    ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks  blocking_locks 
		ON blocking_locks.locktype = blocked_locks.locktype
		AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
		AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
		AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
		AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
		AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
		AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
		AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
		AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
		AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
		AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.GRANTED;


-- ####################
-- cache hit ratio
SELECT blks_hit::float/(blks_read + blks_hit) as cache_hit_ratio
FROM pg_stat_database
WHERE datname=current_database();


-- ####################
-- txn commit ratio
SELECT xact_commit::float/(xact_commit + xact_rollback) as successful_xact_ratio
FROM pg_stat_database
WHERE datname=current_database();


-- ####################
-- index/sequence scan ratio
SELECT sum(idx_scan)/(sum(idx_scan) + sum(seq_scan)) as idx_scan_ratio
FROM pg_stat_all_tables
WHERE schemaname='public';


-- ####################
-- index usage stats
SELECT *,
	tuples_read - tuples_fetched as wasted
FROM (
	SELECT
-- 		t.schemaname,
		t.tablename,
		indexname,
		c.reltuples::BIGINT AS num_rows,
		pg_size_pretty(pg_relation_size(quote_ident(t.schemaname)::text || '.' || quote_ident(t.tablename)::text)) AS table_size,
		pg_size_pretty(pg_relation_size(quote_ident(t.schemaname)::text || '.' || quote_ident(indexrelname)::text)) AS index_size,
		CASE WHEN indisunique THEN 'Y' ELSE 'N' END AS UNIQUE,
		number_of_scans,
		tuples_read,
		tuples_fetched,
		case when tuples_read > 0 then (tuples_fetched::FLOAT / tuples_read) else 0 end as fetch_ratio
	FROM pg_tables t
	LEFT OUTER JOIN pg_class c ON t.tablename = c.relname
	LEFT OUTER JOIN (
		SELECT
			c.relname AS ctablename,
			ipg.relname AS indexname,
			x.indnatts AS number_of_columns,
			idx_scan AS number_of_scans,
			idx_tup_read AS tuples_read,
			idx_tup_fetch AS tuples_fetched,
			indexrelname,
			indisunique,
			schemaname
		FROM pg_index x
		JOIN pg_class c ON c.oid = x.indrelid
		JOIN pg_class ipg ON ipg.oid = x.indexrelid
		JOIN pg_stat_all_indexes psai ON x.indexrelid = psai.indexrelid
	) AS foo ON t.tablename = foo.ctablename AND t.schemaname = foo.schemaname
	WHERE t.schemaname NOT IN ('pg_catalog', 'information_schema')
) t
WHERE TRUE
AND fetch_ratio <= 0.5
-- AND tablename like 'table_name'
-- ORDER BY number_of_scans DESC, num_rows DESC;
-- ORDER BY fetch_ratio DESC;
ORDER BY wasted DESC;


-- ####################
-- detect missing indexes
SELECT
  relname  AS TableName,
  to_char(seq_scan, '999,999,999,999')  AS seq_scan,
  to_char(idx_scan, '999,999,999,999')  AS idx_scan,
  to_char(n_live_tup, '999,999,999,999')  AS table_rows,
  pg_size_pretty(pg_relation_size(relname :: regclass))  AS table_size,
	(CASE WHEN idx_scan > 0 THEN ROUND(100.0 * seq_scan / (seq_scan::decimal + idx_scan), 2) ELSE -1 END) AS percent
FROM pg_stat_all_tables
WHERE schemaname = 'public'
-- AND relname IN ( 'table1', 'table2' )
-- AND n_live_tup > 1000
AND (idx_scan = 0 OR seq_scan::decimal / (seq_scan + idx_scan) >= 0.005)
AND n_live_tup > 1000
-- AND pg_relation_size(relname::regclass) > 500000
-- AND seq_scan > 1000
ORDER BY 6 DESC;


-- ####################
-- detect duplicate indexes
SELECT pg_size_pretty(SUM(pg_relation_size(idx))::BIGINT) AS SIZE,
       (array_agg(idx))[1] AS idx1, (array_agg(idx))[2] AS idx2,
       (array_agg(idx))[3] AS idx3, (array_agg(idx))[4] AS idx4
FROM (
    SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\n'|| indclass::text ||E'\n'|| indkey::text ||E'\n'||
                                         COALESCE(indexprs::text,'')||E'\n' || COALESCE(indpred::text,'')) AS KEY
    FROM pg_index) sub
GROUP BY KEY HAVING COUNT(*)>1
ORDER BY SUM(pg_relation_size(idx)) DESC;


-- ####################
-- check Foreign Keys
select R.TABLE_NAME, R.column_name
from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE u
inner join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS FK
    on U.CONSTRAINT_CATALOG = FK.UNIQUE_CONSTRAINT_CATALOG
    and U.CONSTRAINT_SCHEMA = FK.UNIQUE_CONSTRAINT_SCHEMA
    and U.CONSTRAINT_NAME = FK.UNIQUE_CONSTRAINT_NAME
inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE R
    ON R.CONSTRAINT_CATALOG = FK.CONSTRAINT_CATALOG
    AND R.CONSTRAINT_SCHEMA = FK.CONSTRAINT_SCHEMA
    AND R.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
WHERE U.COLUMN_NAME = 'column_name'
--   AND U.TABLE_CATALOG = 'b'
--   AND U.TABLE_SCHEMA = 'c'
  AND U.TABLE_NAME = 'table_name';
  
