package edu.cmu.cs.db.calcite_app.app;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Optional;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
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
import org.apache.calcite.tools.RelRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QO799Tool {
    CalciteConnection conn;
    CalciteCatalogReader catalog;
    SqlValidator validator;
    RelToSqlConverter rel2sql;
    RelOptCluster cluster = null;
    public static final Logger LOGGER = LoggerFactory.getLogger(QO799Tool.class);

    public QO799Tool(CalciteConnection conn) {
        this.conn = conn;
        CalciteSchema schema = conn.getRootSchema().unwrap(CalciteSchema.class);
        RelDataTypeFactory typeFactory = conn.getTypeFactory();
        this.catalog = new CalciteCatalogReader(
                schema, Collections.singletonList(""), typeFactory,
                conn.config());
        this.validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalog, typeFactory,
                SqlValidator.Config.DEFAULT);
        this.rel2sql = new RelToSqlConverter(SqlDialect.DatabaseProduct.POSTGRESQL.getDialect());
    }

    public Optional<RelNode> planQuery(String inputSql) {
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

        SqlToRelConverter sql2rel = new SqlToRelConverter(
                NOOP_EXPANDER,
                this.validator,
                this.catalog,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());

        RelNode logicalPlan = sql2rel.convertQuery(validNode, false, true).rel;

        explainPlan(logicalPlan, "LOGICAL_PLAN - INITIAL");
        return Optional.of(logicalPlan);
    }

    public Optional<EnumerableRel> optimizePlan(RelNode logicalPlan) {
        RelOptPlanner planner = cluster.getPlanner();

        RelOptUtil.registerDefaultRules(planner, false, false);

        for (RelOptRule rule : EnumerableRules.ENUMERABLE_RULES) {
            planner.addRule(rule);
        }
        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        planner.removeRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);

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
        try {
            RelRunner relRunner = conn.unwrap(RelRunner.class);
            PreparedStatement prepareStmt = relRunner.prepareStatement(physicalPlan);
            ResultSet rs = prepareStmt.executeQuery();
            return Optional.of(rs);
        } catch (Exception e) {
            LOGGER.warn("Error executing plan using `RelRunner`: " + e.getMessage());
            return Optional.empty();
        }
    }

    public String deparseOptimizizedPlanToSql(EnumerableRel physicalPlan) {
        SqlNode sqlNode = this.rel2sql.visitRoot(physicalPlan).asStatement();
        String deparsedSql = sqlNode.toSqlString(SqlDialect.DatabaseProduct.POSTGRESQL.getDialect()).getSql();
        LOGGER.debug("Deparsed SQL: {0}", deparsedSql.replace('\n', ' '));
        return deparsedSql;
    }

    private static void explainPlan(RelNode plan, String header) {
        String planDump = RelOptUtil.dumpPlan("\n==== " + header + " ====", plan, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
        LOGGER.debug(planDump);
    }

    private static RelOptCluster createCluster(RelDataTypeFactory typeFactory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> null;

}
