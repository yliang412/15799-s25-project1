package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import java.nio.file.Files;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelRunner;

public class App {
    private static final Logger LOGGER = Logger.getLogger(App.class.getName());
    static {
        // Change this to `LEVEL.OFF` to disable logging.
        LOGGER.setLevel(Level.ALL);
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%4$-7s] %5$s %n");
        // System.setProperty("java.util.logging.SimpleFormatter.format", "[%4$-7s]
        // %2$s> %5$s %n");
    }

    private static void SerializePlan(RelNode relNode, File outputPath) throws IOException {
        Files.writeString(outputPath.toPath(),
                RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    private static void SerializeResultSet(ResultSet resultSet, File outputPath) throws SQLException, IOException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder resultSetString = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) {
                resultSetString.append(",");
            }
            resultSetString.append(metaData.getColumnName(i));
        }
        resultSetString.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    resultSetString.append(",");
                }
                String s = resultSet.getString(i);
                s = s.replace("\n", "\\n");
                s = s.replace("\r", "\\r");
                s = s.replace("\"", "\"\"");
                resultSetString.append("\"");
                resultSetString.append(s);
                resultSetString.append("\"");
            }
            resultSetString.append("\n");
        }
        Files.writeString(outputPath.toPath(), resultSetString.toString());
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

    private static String readSqlFromFile(String workloadQueriesDir, String queryPath) throws IOException {
        File file = new File(workloadQueriesDir, queryPath);
        return Files.readString(file.toPath());
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: java -jar App.jar <WORKLOAD_QUERIES_DIR> <OUTPUT_DIR>");
            return;
        }

        // Feel free to modify this to take as many or as few arguments as you want.
        System.out.println("Running the app!");
        String workloadQueriesDir = "/home/liangyc/15799-s25-project1/queries";
        String queryPath = args[0];
        System.out.println("queryPath: " + queryPath);
        String outputDir = args[1];
        System.out.println("\toutputDir: " + outputDir);

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        Connection con = DriverManager.getConnection("jdbc:calcite:", connectionProperties);
        CalciteConnection calciteCon = con.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteCon.getRootSchema();
        DataSource dataSource = JdbcSchema.dataSource("jdbc:duckdb:/home/liangyc/15799-s25-project1/test.db",
                "org.duckdb.DuckDBDriver", "", "");
        JdbcSchema duckdbSchema = JdbcSchema.create(rootSchema, "duckdb", dataSource, null, null);
        RelDataTypeFactory typeFactory = calciteCon.getTypeFactory();
        // Set<String> tableNames = duckdbSchema.getTableNames();
        // for (String tableName : tableNames) {
        // Table table = duckdbSchema.getTable(tableName);
        // rootSchema.add(tableName, table);
        // }
        rootSchema = rootSchema.add("duckdb", duckdbSchema);
        Table table = duckdbSchema.getTable("customer");
        RelDataType rowType = table.getRowType(typeFactory);
        LOGGER.info("Row type: " + rowType);

        CalciteCatalogReader catalog = new CalciteCatalogReader(
                rootSchema.unwrap(CalciteSchema.class), new ArrayList<>(), typeFactory,
                calciteCon.config());
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalog, typeFactory,
                SqlValidator.Config.DEFAULT);

        String inputSql = readSqlFromFile(workloadQueriesDir, queryPath);
        SqlParser parser = SqlParser.create(inputSql);

        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRule(CoreRules.PROJECT_TO_CALC);
        planner.addRule(CoreRules.FILTER_TO_CALC);
        planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_UNION_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_MINUS_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_INTERSECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_MATCH_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_WINDOW_RULE);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        SqlToRelConverter converter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalog,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());

        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        } catch (SqlParseException e) {
            System.out.println("Error parsing SQL: " + e.getMessage());
            return;
        }

        LOGGER.info(MessageFormat.format("Parsed: {0}", sqlNode.toString().replace('\n', ' ')));

        SqlNode validNode;
        try {
            validNode = validator.validate(sqlNode);
        } catch (Exception e) {
            System.out.println("Error validating SQL: " + e.getMessage());
            return;
        }
        LOGGER.info(MessageFormat.format("Validated: {0}", validNode.toString().replace('\n', ' ')));

        RelNode logicalPlan = converter.convertQuery(validNode, false, true).rel;
        String planDump = RelOptUtil.dumpPlan("\n[logical_plan.initial]", logicalPlan, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
        LOGGER.info(planDump);

        // Define the type of the output plan (in this case we want a physical plan in
        // EnumerableContention)
        logicalPlan = planner.changeTraits(logicalPlan,
                cluster.traitSet().replace(EnumerableConvention.INSTANCE));
        planDump = RelOptUtil.dumpPlan("\n[logical_plan.after_trait_change]", logicalPlan, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
        LOGGER.info(planDump);
        planner.setRoot(logicalPlan);
        // Start the optimization process to obtain the most efficient physical plan
        // based on the
        // provided rule set.
        EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();

        // Display the physical plan
        planDump = RelOptUtil.dumpPlan("\n[physical_plan]", phyPlan, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
        LOGGER.info(planDump);

        RelRunner relRunner = calciteCon.unwrap(RelRunner.class);
        PreparedStatement prepareStmt = relRunner.prepareStatement(phyPlan);
        ResultSet resultSet = prepareStmt.executeQuery();
        SerializeResultSet(resultSet, new File("/home/liangyc/15799-s25-project1/", "result.csv"));

        String deparsedSql = sqlNode.toSqlString(SqlDialect.DatabaseProduct.POSTGRESQL.getDialect()).getSql()
                .replace('\n', ' ');
        LOGGER.info(MessageFormat.format("Deparsed SQL: {0}", deparsedSql));

        // Note: in practice, you would probably use
        // org.apache.calcite.tools.Frameworks.
        // That package provides simple defaults that make it easier to configure
        // Calcite.
        // But there's a lot of magic happening there; since this is an educational
        // project,
        // we guide you towards the explicit method in the writeup.
    }
}
