SELECT c.c_custkey, c.c_name
FROM customer c
JOIN orders o1 ON c.c_custkey = o1.o_custkey
JOIN orders o2 ON c.c_custkey = o2.o_custkey
WHERE o1.o_orderstatus = 'F'
  AND o2.o_orderpriority = '1-URGENT'
  AND EXISTS (
    SELECT 1 
    FROM lineitem l 
    WHERE l.l_orderkey = o1.o_orderkey 
    AND l.l_quantity > 30
  )