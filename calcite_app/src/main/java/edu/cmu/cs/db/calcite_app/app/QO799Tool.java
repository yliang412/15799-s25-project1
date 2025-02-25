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
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Programs;
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
                .build());

        RelNode logicalPlan = pair.plan;
        planner.setRoot(logicalPlan);
        RelNode secondVersion = planner.findBestExp();

        explainPlan(secondVersion, "SECOND VERSION");

        RelNode decorrelated = this.sql2rel.decorrelate(pair.validNode, pair.plan);

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
        RelOptPlanner planner = cluster.getPlanner();

        // RelOptUtil.registerDefaultRules(planner, false, false);

        planner.addRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
        planner.addRule(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
        planner.addRule(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
        planner.addRule(CoreRules.PROJECT_OVER_SUM_TO_SUM0_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);

        Programs.RULE_SET.forEach(planner::addRule);
        planner.addRule(CoreRules.PROJECT_JOIN_REMOVE);
        planner.addRule(CoreRules.JOIN_PUSH_EXPRESSIONS);
        planner.removeRule(CoreRules.JOIN_ASSOCIATE);
        planner.removeRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
        planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);

        logicalPlan = planner.changeTraits(logicalPlan,
                cluster.traitSet().replace(EnumerableConvention.INSTANCE));

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

    public Optional<ResultSet> executeEnumerablePlan(EnumerableRel physicalPlan) {
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
            return future.get(15, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOGGER.warn("Timeout executing plan");
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public String deparseOptimizizedPlanToSql(EnumerableRel physicalPlan) {
        SqlNode sqlNode = this.rel2sql.visitRoot(physicalPlan).asStatement();
        String deparsedSql = sqlNode.toSqlString(SqlDialect.DatabaseProduct.POSTGRESQL.getDialect()).getSql();
        LOGGER.debug("Deparsed SQL: {}", deparsedSql.replace('\n', ' '));
        return deparsedSql;
    }

    private static void explainPlan(RelNode plan, String header, Level level) {
        String planDump = RelOptUtil.dumpPlan("\n==== " + header + " ====", plan, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
        LOGGER.atLevel(level).log(planDump);
    }

    private static void explainPlan(RelNode plan, String header) {
        explainPlan(plan, header, Level.DEBUG);
    }

    private static RelOptCluster createCluster(RelDataTypeFactory typeFactory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> null;

}
