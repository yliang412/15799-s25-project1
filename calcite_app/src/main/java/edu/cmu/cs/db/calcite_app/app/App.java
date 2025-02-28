package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
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

    private static class QueryIO {
        public File input;
        public String output_base;
        public File outputDir;

        public QueryIO(File input, File outputDir) {
            this.input = input;
            this.outputDir = outputDir;
            this.output_base = input.getName().replace(".sql", "");
        }

        public String readInputSql() throws IOException {
            return Files.readString(input.toPath());
        }

        public File getOutputFile(String suffix) {
            return new File(outputDir, output_base + suffix);
        }

        public void writeOriginalSql(String originalSql) throws IOException {
            File file = getOutputFile(".sql");
            Files.writeString(file.toPath(), originalSql);
        }

        public void writeLogicalPlan(RelNode logicalPlan) throws IOException {
            File file = getOutputFile(".txt");
            SerializePlan(logicalPlan, file);
        }

        public void writeOptimizedPlan(RelNode optimizedPlan) throws IOException {
            File file = getOutputFile("_optimized.txt");
            SerializePlan(optimizedPlan, file);
        }

        public void writeOptimizedSql(String optimizedSql) throws IOException {
            File file = getOutputFile("_optimized.sql");
            Files.writeString(file.toPath(), optimizedSql);
        }

        public void writeResultSet(ResultSet resultSet) throws SQLException, IOException {
            File file = getOutputFile("_results.csv");
            SerializeResultSet(resultSet, file);
        }
    }

    private static List<String> discoverInputFileNames(File queriesDir) {
        List<String> inputFiles = new ArrayList<>();
        for (File file : queriesDir.listFiles()) {
            if (file.isFile() && file.getName().endsWith(".sql")) {
                inputFiles.add(file.getName());
            }
        }
        inputFiles.sort((f1Name, f2Name) -> {
            if (f1Name.startsWith("q") && f2Name.startsWith("q")) {
                int f1Num = Integer.parseInt(f1Name.substring(1, f1Name.indexOf('.')));
                int f2Num = Integer.parseInt(f2Name.substring(1, f2Name.indexOf('.')));
                return Integer.compare(f1Num, f2Num);
            }
            return f1Name.compareTo(f2Name);
        });

        LOGGER.info("Discovered {} input SQL queries", inputFiles.size());
        return inputFiles;
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Running the app!");

        File queriesDir = new File(args[0]);
        LOGGER.info("queriesDir: {}", queriesDir.getCanonicalPath());
        File outputDir = new File(args[1]);
        LOGGER.info("outputDir: {}", outputDir.getCanonicalPath());

        List<String> inputFileNames = discoverInputFileNames(queriesDir);
        // List<String> inputFileNames = new ArrayList<>();
        // inputFileNames.add("q4.sql");

        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnection conn = DriverManager.getConnection("jdbc:calcite:", props).unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = conn.getRootSchema();
        String duckdbUrl = MessageFormat.format("jdbc:duckdb:{0}/15799-p1.db", App.projectRoot.getCanonicalPath());
        RelDataTypeFactory typeFactory = conn.getTypeFactory();
        DuckDbLoader.load(rootSchema, typeFactory, duckdbUrl);

        QO799Tool tool = new QO799Tool(conn);

        for (String inputFileName : inputFileNames) {
            LOGGER.info("===== PROCESSING {} =====", inputFileName);
            File inputFile = new File(queriesDir, inputFileName);
            QueryIO io = new QueryIO(inputFile, outputDir);
            String query = io.readInputSql();
            io.writeOriginalSql(query);
            tool.planQuery(query).ifPresent(pair -> {
                try {
                    io.writeLogicalPlan(pair.plan);
                } catch (IOException e) {
                    LOGGER.warn("[{}] Failed to serialize logical plan: {}", e.getMessage());
                }

                RelNode afterRewrite = tool.rewritePlan(pair);
                tool.optimizePlan(afterRewrite).ifPresent(optimizedPlan -> {
                    pair.plan = optimizedPlan;
                    RelNode decorrelatePlan = tool.decorrelatePlan(pair);
                    try {
                        io.writeOptimizedPlan(decorrelatePlan);
                    } catch (IOException e) {
                        LOGGER.warn("Failed to serialize optimized physical plan: " + e.getMessage());
                    }
                    tool.executeEnumerablePlan(decorrelatePlan).ifPresent(resultSet -> {
                        try {
                            io.writeResultSet(resultSet);
                        } catch (SQLException | IOException e) {
                            LOGGER.warn("Failed to serialize result set: " + e.getMessage());
                        }
                    });
                    String deparsedSql = tool.deparseOptimizizedPlanToSql(optimizedPlan);
                    try {
                        io.writeOptimizedSql(deparsedSql);
                    } catch (IOException e) {
                        LOGGER.warn("Failed to serialize deparsed SQL: " + e.getMessage());
                    }
                });

            });
            LOGGER.info("===== DONE {} =====", inputFileName);
        }

        // Note: in practice, you would probably use
        // org.apache.calcite.tools.Frameworks.
        // That package provides simple defaults that make it easier to configure
        // Calcite.
        // But there's a lot of magic happening there; since this is an educational
        // project,
        // we guide you towards the explicit method in the writeup.
    }

}
