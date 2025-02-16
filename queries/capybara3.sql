SELECT o_orderpriority,
       COUNT(DISTINCT l_partkey) as unique_parts,
       AVG(l_quantity) as avg_quantity,
       SUM(l_quantity)/COUNT(l_quantity) as computed_avg
FROM orders o
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY o_orderpriority
HAVING AVG(l_quantity) > 25 
   AND o_orderpriority LIKE '1%'