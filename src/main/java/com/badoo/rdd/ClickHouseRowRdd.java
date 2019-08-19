package com.badoo.rdd;

import com.badoo.IConnectionFactory;
import com.badoo.IStreamObjectReader;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;

/**
 * Created by krash on 19.08.19.
 */
public class ClickHouseRowRdd extends ClickHouseBinaryRdd<Row> {
    public ClickHouseRowRdd(SparkContext ctx, IConnectionFactory factory, String sql, IStreamObjectReader<Row> objectReader) {
        super(ctx, Row.class, factory, sql, objectReader);
    }
}
