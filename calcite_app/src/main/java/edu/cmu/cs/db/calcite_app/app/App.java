package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;
import java.nio.file.Files;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    public static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    private static final File projectRoot = new File("..");

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

    public static void main(String[] args) throws Exception {
        System.out.println("Running the app!");

        File queriesDir = new File(args[0]);
        System.out.println("\tqueriesDir: " + queriesDir.getCanonicalPath());
        File outputDir = new File(args[1]);
        System.out.println("\toutputDir: " + outputDir.getCanonicalPath());

        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnection conn = DriverManager.getConnection("jdbc:calcite:", props).unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = conn.getRootSchema();
        String duckdbUrl = MessageFormat.format("jdbc:duckdb:{0}/15799-p1.db", App.projectRoot.getCanonicalPath());
        RelDataTypeFactory typeFactory = conn.getTypeFactory();
        DuckDbLoader.load(rootSchema, typeFactory, duckdbUrl);

        QO799Tool tool = new QO799Tool(conn);

        File queryFile = new File(queriesDir, "q1.sql");
        String query = Files.readString(queryFile.toPath());
        File outputQueryFile = new File(outputDir, "q1.sql");
        Files.writeString(outputQueryFile.toPath(), query);
        tool.planQuery(query).ifPresent(logicalPlan -> {
            try {
                File outputLogicalPlanFile = new File(outputDir, "q1.txt");
                SerializePlan(logicalPlan, outputLogicalPlanFile);
            } catch (IOException e) {
                LOGGER.warn("Failed to serialize logical plan: " + e.getMessage());
            }
            tool.optimizePlan(logicalPlan).ifPresent(physicalPlan -> {
                try {
                    File outputPhysicalPlanFile = new File(outputDir, "q1_optimized.txt");
                    SerializePlan(physicalPlan, outputPhysicalPlanFile);
                } catch (IOException e) {
                    LOGGER.warn("Failed to serialize optimized physical plan: " + e.getMessage());
                }
                tool.executeEnumerablePlan(physicalPlan).ifPresent(resultSet -> {
                    File resultFile = new File(outputDir, "q1_results.csv");
                    try {
                        SerializeResultSet(resultSet, resultFile);
                    } catch (SQLException | IOException e) {
                        LOGGER.warn("Failed to serialize result set: " + e.getMessage());
                    }
                });
                String deparsedSql = tool.deparseOptimizizedPlanToSql(physicalPlan);
                File deparsedFile = new File(outputDir, "q1_optimized.sql");
                try {
                    Files.writeString(deparsedFile.toPath(), deparsedSql);
                } catch (IOException e) {
                    LOGGER.warn("Failed to serialize deparsed SQL: " + e.getMessage());
                }
            });
        });

        // Note: in practice, you would probably use
        // org.apache.calcite.tools.Frameworks.
        // That package provides simple defaults that make it easier to configure
        // Calcite.
        // But there's a lot of magic happening there; since this is an educational
        // project,
        // we guide you towards the explicit method in the writeup.
    }

}
