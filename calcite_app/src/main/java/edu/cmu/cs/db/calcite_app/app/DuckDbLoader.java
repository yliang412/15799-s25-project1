package edu.cmu.cs.db.calcite_app.app;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import java.sql.Statement;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuckDbLoader {
    public static final Logger LOGGER = LoggerFactory.getLogger(DuckDbLoader.class);

    public static void load(SchemaPlus rootSchema, RelDataTypeFactory typeFactory, String url) throws Exception {
        Map<String, RelDataType> tableRowTypes = extractTableInfo(rootSchema, typeFactory, url);
        LOGGER.trace("Extracted table info: " + tableRowTypes);
        System.gc();
        DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection(url,
                "", "");

        for (Map.Entry<String, RelDataType> entry : tableRowTypes.entrySet()) {
            String tableName = entry.getKey();
            RelDataType rowType = entry.getValue();

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);

            int defaultCapacity = 1000; // default size
            List<Object[]> rows = new ArrayList<>(defaultCapacity);

            ResultSetMetaData metadata = rs.getMetaData();
            int columnCount = metadata.getColumnCount();

            while (rs.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                rows.add(row);
            }

            // Create in-memory table with the loaded data
            MemoryTable inMemoryTable = new MemoryTable(rowType, rows);
            rootSchema.add(tableName, inMemoryTable);
        }
        LOGGER.debug("Done loading all tables");
    }

    private static Map<String, RelDataType> extractTableInfo(SchemaPlus rootSchema, RelDataTypeFactory typeFactory,
            String url) throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        DataSource dataSource = JdbcSchema.dataSource(url, "org.duckdb.DuckDBDriver", "", "");
        JdbcSchema duckdbSchema = JdbcSchema.create(rootSchema, "duckdb", dataSource, null, null);

        Set<String> tableNames = duckdbSchema.getTableNames();

        Map<String, RelDataType> tableRowTypes = new HashMap<>();
        for (String tableName : tableNames) {
            Table table = duckdbSchema.getTable(tableName);
            RelDataType rowType = table.getRowType(typeFactory);
            tableRowTypes.put(tableName, rowType);
        }
        return tableRowTypes;
    }

}
