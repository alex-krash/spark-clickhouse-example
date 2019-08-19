package com.badoo.rdd;

import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryInputStream;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * Created by krash on 19.08.19.
 */
public abstract class ClickHouseBinaryIterator<T> implements Iterator<T> {

    private final Connection connection;
    private final ClickHouseStatement statement;
    private final ClickHouseRowBinaryInputStream stream;
    private final long count;
    private boolean closed = false;
    private long read = 0;

    protected ClickHouseBinaryIterator(Connection connection, ClickHouseStatement statement, ClickHouseRowBinaryInputStream stream, long count) {
        this.connection = connection;
        this.statement = statement;
        this.stream = stream;
        this.count = count;
    }

    public void closeIfNeeded() {
        if (!closed) {
            closed = true;
            close();
        }
    }

    @Override
    public boolean hasNext() {
        return read < count;
    }

    @Override
    public T next() {
        read++;
        try {
            return read(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract T read(ClickHouseRowBinaryInputStream stream) throws IOException;

    /**
     * Looks scary, but it is a copy-paste of Spark interal code
     */
    protected void close() {
        if (null != stream) {
            try {
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (null != statement) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
