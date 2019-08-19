package com.badoo;

import ru.yandex.clickhouse.util.ClickHouseRowBinaryInputStream;

import java.io.IOException;
import java.io.Serializable;

/**
 * An interface for factories, producing
 * Extends Serializable in order to be encoded in lambda
 */
public interface IStreamObjectReader<T> extends Serializable {
    public T read(ClickHouseRowBinaryInputStream inputStream) throws IOException;
}
