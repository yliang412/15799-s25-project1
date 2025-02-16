SELECT l1.l_orderkey, l1.l_quantity
FROM lineitem l1, lineitem l2
WHERE l1.l_orderkey = l2.l_orderkey
  AND l2.l_quantity > 30