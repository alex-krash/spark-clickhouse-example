package com.badoo;

import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Created by krash on 19.08.19.
 */
public interface IConnectionFactory extends Supplier<ClickHouseConnection>, Serializable {
}
