package edu.cmu.cs.db.calcite_app.app;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import com.google.common.collect.ImmutableList;

public class QO799Tool {
    CalciteConnection conn;
    CalciteCatalogReader catalog;
    SqlValidator validator;
    RelToSqlConverter rel2sql;
    RelOptCluster cluster = null;
    SqlToRelConverter sql2rel = null;

    public static final Logger LOGGER = LoggerFactory.getLogger(QO799Tool.class);

    public final class SqlRelPair {
        public SqlNode validNode;
        public RelNode plan;

        public SqlRelPair(SqlNode validNode, RelNode plan) {
            this.validNode = validNode;
            this.plan = plan;
        }
    }

    public QO799Tool(CalciteConnection conn) {
        this.conn = conn;
        CalciteSchema schema = conn.getRootSchema().unwrap(CalciteSchema.class);
        RelDataTypeFactory typeFactory = conn.getTypeFactory();
        this.catalog = new CalciteCatalogReader(
                schema, Collections.singletonList(""), typeFactory,
                conn.config());
        this.validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalog, typeFactory,
                SqlValidator.Config.DEFAULT);
        this.rel2sql = new RelToSqlConverter(SqlDialect.DatabaseProduct.FIREBOLT.getDialect());
    }

    public RelNode decorrelatePlan(SqlRelPair pair) {
        RelNode decorrelated = RelDecorrelator.decorrelateQuery(pair.plan,
                RelBuilder.create(Frameworks.newConfigBuilder().build()));
        explainPlan(decorrelated, "DECORRELATED");
        return decorrelated;
    }

    public Optional<SqlRelPair> planQuery(String inputSql) {
        SqlParser parser = SqlParser.create(inputSql);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        } catch (Exception e) {
            LOGGER.warn("Failed to parse query: " + e.getMessage());
            return Optional.empty();
        }
        LOGGER.debug("Parsed: " + sqlNode.toString().replace('\n', ' '));

        SqlNode validNode;
        try {
            validNode = this.validator.validate(sqlNode);
        } catch (Exception e) {
            LOGGER.warn("Error validating SQL: " + e.getMessage());
            return Optional.empty();
        }

        this.cluster = createCluster(this.validator.getTypeFactory());
        this.sql2rel = new SqlToRelConverter(
                NOOP_EXPANDER,
                this.validator,
                this.catalog,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config().withExpand(true));

        RelNode logicalPlan = this.sql2rel.convertQuery(validNode, false, true).rel;

        explainPlan(logicalPlan, "LOGICAL_PLAN - INITIAL");
        return Optional.of(new SqlRelPair(validNode, logicalPlan));
    }

    public RelNode rewritePlan(SqlRelPair pair) {
        RelOptPlanner planner = new HepPlanner(HepProgram.builder()
                .addCommonRelSubExprInstruction()
                .addRuleCollection(
                        ImmutableList.of(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
                                CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE,
                                CoreRules.PROJECT_OVER_SUM_TO_SUM0_RULE))
                .addRuleInstance(CoreRules.PROJECT_CORRELATE_TRANSPOSE)
                .addRuleInstance(CoreRules.FILTER_CORRELATE)
                .build());

        RelNode logicalPlan = pair.plan;
        planner.setRoot(logicalPlan);
        RelNode secondVersion = planner.findBestExp();

        explainPlan(secondVersion, "SECOND VERSION");

        RelNode decorrelated = this.decorrelatePlan(pair);

        explainPlan(decorrelated, "DECORRELATED");

        planner = new HepPlanner(HepProgram.builder().addCommonRelSubExprInstruction()
                .addRuleInstance(CoreRules.PROJECT_JOIN_REMOVE)
                .addRuleInstance(CoreRules.PROJECT_JOIN_JOIN_REMOVE)
                .addRuleInstance(CoreRules.PROJECT_MERGE)
                .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                .addRuleInstance(CoreRules.JOIN_PUSH_EXPRESSIONS)
                .addSubprogram(HepProgram.builder()
                        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
                        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN).build())
                .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE)
                .addCommonRelSubExprInstruction()
                .build());
        planner.setRoot(decorrelated);

        RelNode afterHeuristics = planner.findBestExp();

        explainPlan(afterHeuristics, "AFTER_HEURISTICS");
        return afterHeuristics;
    }

    public Optional<EnumerableRel> optimizePlan(RelNode logicalPlan) {
        RelOptPlanner planner = this.cluster.getPlanner();

        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_CORRELATE_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);

        planner.addRule(CoreRules.PROJECT_TO_CALC);
        planner.addRule(CoreRules.FILTER_INTO_JOIN);
        planner.addRule(CoreRules.FILTER_PROJECT_TRANSPOSE);
        planner.addRule(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
        planner.addRule(CoreRules.FILTER_MERGE);
        planner.addRule(CoreRules.SEMI_JOIN_FILTER_TRANSPOSE);
        planner.addRule(CoreRules.JOIN_TO_SEMI_JOIN);
        planner.addRule(CoreRules.SEMI_JOIN_JOIN_TRANSPOSE);
        planner.addRule(CoreRules.PROJECT_AGGREGATE_MERGE);
        planner.addRule(CoreRules.PROJECT_FILTER_VALUES_MERGE);
        planner.addRule(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS);
        planner.addRule(CoreRules.PROJECT_REMOVE);
        planner.addRule(CoreRules.PROJECT_JOIN_TRANSPOSE);
        planner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        planner.addRule(CoreRules.JOIN_REDUCE_EXPRESSIONS);
        planner.addRule(CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
        planner.addRule(CoreRules.AGGREGATE_PROJECT_MERGE);
        planner.addRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE);
        planner.addRule(CoreRules.SORT_PROJECT_TRANSPOSE);
        planner.addRule(CoreRules.JOIN_EXTRACT_FILTER);
        planner.addRule(CoreRules.FILTER_INTERPRETER_SCAN);
        planner.addRule(PruneEmptyRules.AGGREGATE_INSTANCE);
        planner.addRule(PruneEmptyRules.FILTER_INSTANCE);
        planner.addRule(PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE);
        planner.addRule(PruneEmptyRules.SORT_INSTANCE);
        planner.addRule(PruneEmptyRules.PROJECT_INSTANCE);
        planner.addRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
        planner.addRule(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
        planner.addRule(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
        planner.addRule(CoreRules.PROJECT_OVER_SUM_TO_SUM0_RULE);
        planner.addRule(CoreRules.JOIN_COMMUTE_OUTER);
        planner.addRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);
        planner.addRule(JoinPushThroughJoinRule.RIGHT);
        planner.addRule(JoinPushThroughJoinRule.LEFT);

        logicalPlan = planner.changeTraits(logicalPlan,
                this.cluster.traitSet().replace(EnumerableConvention.INSTANCE));

        planner.setRoot(logicalPlan);

        EnumerableRel physicalPlan;
        try {
            physicalPlan = (EnumerableRel) planner.findBestExp();
        } catch (Exception e) {
            LOGGER.warn("Error finding best plan: " + e.getMessage());
            return Optional.empty();
        }

        explainPlan(physicalPlan, "PHYSICAL_PLAN");

        return Optional.of(physicalPlan);
    }

    public Optional<ResultSet> executeEnumerablePlan(RelNode physicalPlan) {
        CompletableFuture<Optional<ResultSet>> future = CompletableFuture.supplyAsync(() -> {
            try {
                RelRunner relRunner = conn.unwrap(RelRunner.class);
                PreparedStatement prepareStmt = relRunner.prepareStatement(physicalPlan);
                ResultSet rs = prepareStmt.executeQuery();
                return Optional.of(rs);
            } catch (Exception e) {
                LOGGER.warn("Error executing plan using `RelRunner`: " + e.getMessage());
                return Optional.empty();
            }
        });

        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOGGER.warn("Timeout executing plan");
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public String deparseOptimizizedPlanToSql(RelNode physicalPlan) {
        SqlNode sqlNode = this.rel2sql.visitRoot(physicalPlan).asStatement();
        String deparsedSql = sqlNode.toSqlString(SqlDialect.DatabaseProduct.POSTGRESQL.getDialect()).getSql();
        LOGGER.debug("Deparsed SQL: {}", deparsedSql.replace('\n', ' '));
        return deparsedSql;
    }

    public static void explainPlan(RelNode plan, String header, Level level) {
        String planDump = RelOptUtil.dumpPlan("\n==== " + header + " ====", plan, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
        LOGGER.atLevel(level).log(planDump);
    }

    public static void explainPlan(RelNode plan, String header) {
        explainPlan(plan, header, Level.DEBUG);
    }

    private static RelOptCluster createCluster(RelDataTypeFactory typeFactory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        // planner.addListener(new QO799RelOptListener());
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> null;

    @SuppressWarnings("unused")
    private static class QO799RelOptListener implements RelOptListener {

        public QO799RelOptListener() {
        }

        @Override
        public void relEquivalenceFound(RelEquivalenceEvent event) {
        }

        @Override
        public void ruleAttempted(RuleAttemptedEvent event) {
        }

        @Override
        public void ruleProductionSucceeded(RuleProductionEvent event) {
            LOGGER.info("Rule succeeded: " + event.getRuleCall().getRule().toString());
        }

        @Override
        public void relDiscarded(RelDiscardedEvent event) {
        }

        @Override
        public void relChosen(RelChosenEvent event) {
        }

    }

}
