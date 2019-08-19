package com.badoo.rdd;

import com.badoo.IConnectionFactory;
import com.badoo.IStreamObjectReader;
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.JdbcPartition;
import org.apache.spark.rdd.RDD;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryInputStream;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

/**
 * A representation of Spark RDD, reading data from
 * ClickHouse RowBinary stream, and producing objects
 */
public class ClickHouseBinaryRdd<T> extends RDD<T> {

    private final IConnectionFactory factory;
    private final String sql;
    private final IStreamObjectReader<T> objectReader;
    private Long rowCount = null;

    public ClickHouseBinaryRdd(SparkContext ctx, Class<T> elementType, IConnectionFactory factory, String sql, IStreamObjectReader<T> objectReader) {
        super(ctx, JavaConversions.<Dependency<?>>asScalaBuffer(Collections.emptyList()).toSeq(), ClassTag$.MODULE$.apply(elementType));
        this.factory = factory;
        this.sql = sql;
        this.objectReader = objectReader;
    }

    /**
     * Get bound of stream
     */
    private long getRowCount() throws SQLException {
        long cnt;

        if (null == rowCount) {
            try (ClickHouseConnection connection = factory.get();
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT count() FROM (" + sql + ")")) {
                rs.next();
                cnt = rs.getLong(1);
            }
        } else {
            cnt = rowCount;
        }
        return cnt;
    }

    /**
     * Method for setting up number of rows, if known
     */
    public void setRowCount(Long rowCount) {
        if (null != rowCount && rowCount < 0) {
            throw new IllegalArgumentException("Row count can not be less than zero");
        }
        this.rowCount = rowCount;
    }

    @Override
    public Iterator<T> compute(Partition partition, TaskContext context) {

        try {
            // get stream bounds
            long rowCount = getRowCount();

            java.util.Iterator<T> result;
            if (0 == rowCount) {
                result = Collections.emptyIterator();
            } else {
                ClickHouseConnection connection = factory.get();
                final ClickHouseStatement statement = connection.createStatement();
                ClickHouseBinaryIterator<T> temp = new ClickHouseBinaryIterator<T>(
                        connection,
                        statement,
                        statement.executeQueryClickhouseRowBinaryStream(sql),
                        rowCount) {
                    @Override
                    protected T read(ClickHouseRowBinaryInputStream stream) throws IOException {
                        return objectReader.read(stream);
                    }
                };

                context.addTaskCompletionListener(context1 -> {
                    temp.closeIfNeeded();
                });
                result = temp;
            }
            return JavaConversions.asScalaIterator(result);
        } catch (SQLException error) {
            throw new RuntimeException("SQL error", error);
        }
    }

    @Override
    public Partition[] getPartitions() {
        return new Partition[]{new JdbcPartition(0, 0, 0)};
    }
}
