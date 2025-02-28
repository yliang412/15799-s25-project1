# Reflection

## Setting up catalog and tables

I tried to make DuckDB JDBC Schema work directly with Calcite but end up creating new `InMemoryTable` by copying over schema and data information. I also provided the base table row count statistics.

## Optimization Pipeline

The overall optimization pipeline has a few stages. The parsing, validation, binding, and deparsing are all taken care of by using relevant Calcite APIs. The following sections would only describe the two Rewrite phases.

```
                                validated           
sql  +-------+ ast +----------+    ast     +------+  
-->  | Parse | --> | Validate | ---------> | Bind | ---*
     +-------+     +----------+            +------+

logical                 logical  +----------------+  physical
 plan    +-----------+   plan    |     Unified    |    plan   +-----------+
*------> | Rewrite 1 | --------> | Volcano Search | --------> | Rewrite 2 | ---*
         +-----------+           +----------------+           +-----------+
            
                +----------+ ast +---------+ sql +--------+
 physical +---> | RelToAst | --> | Deparse | --> | DuckDB |
   plan   |     +----------+     +---------+     +--------+
*---------+
          |     +-----------+ 
          +---> | RelRunner |
                +-----------+
```

### Rewrite Phases

In both of the rewrite phases, I uses the same set of rules. It is interesting to investigate whether it make sense to have different rewrite rules for before and after unified search.

The rewrite phase also handles rewriting correlated subquery expressions into the `Correlate` plan node and then invoke the decorrelator to get decorrelated plan. I think it works pretty well to eliminate the correlations in the queries.

The rewrite phase also includes a `MultiJoin` optimization. This is used to consider > 2 joins at the same time. This might not be super beneficial to TPC-H since the number of relations in the queries are still pretty small.

I think the rule group idea in the `HepPlanner` is pretty nit that you can specified the next thing to in a sequence. I think it would be neat if the `VolcanoPlanner` also has the ability to use this for guidance.

### Unified Search


In the unified search phase, I added both transformation rules and implementation rules to do the search. 

One of the optimization that took me a lot of time to figure out is swtiching from `JOIN_COMMUTE` to `JOIN_COMMUTE_OUTER` that possibly reorder the outer joins as well.


One interesting problem I run into has to do with the `PROJECT_MERGE` rule. With `PROJECT_MERGE` turned on with some other rule, I experienced a dead loop in the optimization of `q7.sql`. I validated this is the case by setting up my `RelOptListener` and observes that `PROJECT_MERGE` is keep getting applied and I cannot get out of the optimization. This indicates that we need to be careful with rules that remove expression from a subtree since they lead to group merging. I end up disabled that rule.
