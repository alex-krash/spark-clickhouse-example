package com.badoo;

import avro.shaded.com.google.common.collect.Lists;
import com.badoo.rdd.ClickHouseBinaryRdd;
import com.badoo.rdd.ClickHouseRowRdd;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by krash on 19.08.19.
 */
public class Example {

    // query to send to clickhouse
    private static final String QUERY = "SELECT toDateTime(number), number FROM numbers(10000)";
    // instruction to read a single row from a stream
    // it is possible to generalize it, to perform automated schema inferring,
    // like SparkSQL does
    private static final IStreamObjectReader<Row> STREAM_OBJECT_READER =
            (IStreamObjectReader<Row>) inputStream -> RowFactory.create(
                    inputStream.readDateTime(),
                    inputStream.readUInt64AsLong()
            );
    private static final StructType SCHEMA = DataTypes.createStructType(
            Lists.newArrayList(
                    DataTypes.createStructField("ts", DataTypes.TimestampType, false),
                    DataTypes.createStructField("number", DataTypes.LongType, false)
            )
    );

    public static void main(String[] args) {
        // using fancy interface to connect ClickHouse
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUseServerTimeZone(false);
        properties.setUseTimeZone("UTC");

        // in order to serialize connection configuration, and pass to worker,
        // perform itermediate convertation
        Properties serializableProperties = properties.asProperties();
        String connectionURL = "jdbc:clickhouse://localhost:8123";

        // factory, creating connection on worker node
        final IConnectionFactory connectionFactory = (IConnectionFactory) () -> {
            ClickHouseDataSource ds = new ClickHouseDataSource(connectionURL, serializableProperties);
            try {
                return ds.getConnection();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

        JavaSparkContext ctx = new JavaSparkContext("local[*]", "ClickHouseRowBinary");
        // instance of RDD, that deals with rows
        ClickHouseBinaryRdd<Row> rdd = new ClickHouseRowRdd(
                ctx.sc(),
                connectionFactory,
                QUERY,
                STREAM_OBJECT_READER);
        // converting to dataset
        Dataset<Row> dataset = new SparkSession(ctx.sc()).createDataFrame(rdd, SCHEMA);
        // work with dataset as usually
        dataset.printSchema();
        dataset.show();
    }
}
