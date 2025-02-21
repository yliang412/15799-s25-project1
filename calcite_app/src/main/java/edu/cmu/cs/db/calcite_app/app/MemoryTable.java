package edu.cmu.cs.db.calcite_app.app;

import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * A table that stores data in memory.
 * 
 * It implements the {@link ScannableTable} interface, which allows it to be
 * scanned
 */
public class MemoryTable extends AbstractTable implements ScannableTable {

    /** The description of the attributes. */
    private final RelDataType rowType;
    /** The table data stored in memory. */
    private final List<Object[]> rows;

    private class TrueCardStatistic implements Statistic {
        Double rowCount;

        public TrueCardStatistic(int rowCount) {
            this.rowCount = Double.valueOf(rowCount);
        }

        @Override
        public Double getRowCount() {
            return rowCount;
        }
    }

    /**
     * Creates a MemoryTable.
     *
     * @param rowType the description of the attributes
     * @param rows    the table data stored in memory
     */
    public MemoryTable(RelDataType rowType, List<Object[]> rows) {
        this.rowType = rowType;
        this.rows = rows;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return this.rowType;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(this.rows);
    }

    @Override
    public Statistic getStatistic() {
        return new TrueCardStatistic(rows.size());
    }
}