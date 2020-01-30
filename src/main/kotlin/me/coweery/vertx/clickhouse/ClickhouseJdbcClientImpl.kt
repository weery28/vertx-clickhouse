package me.coweery.vertx.clickhouse

import io.reactivex.Completable
import io.reactivex.Single
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.reactivex.core.Vertx
import ru.yandex.clickhouse.ClickHouseArray
import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.util.ClickHouseArrayUtil
import java.sql.ResultSet
import java.sql.Timestamp
import java.util.TimeZone
import java.util.UUID

class ClickhouseJdbcClientImpl(
    private val vertx: Vertx,
    private val properties: ClickhouseProperties
) : ClickhouseClient {

    companion object {
        private const val MAPPING_SIZE = 8192
        private const val CLICKHOUSE_JDBC_PREFFIX = "jdbc:clickhouse://"
    }

    private val connection =
        ClickHouseDataSource("$CLICKHOUSE_JDBC_PREFFIX${properties.host}:${properties.port}/${properties.database}")
            .getConnection(properties.username, properties.password)

    override fun <T : Any> fetchOne(sql: String, resultClass: Class<T>): Single<T> {

        return query(sql) { it.mapTo(resultClass) }.map { it.first() }.onErrorResumeNext {
            if (it is NoSuchElementException) {
                Single.error(NullPointerException())
            } else {
                Single.error(it)
            }
        }
    }

    override fun fetchOne(sql: String): Single<JsonObject> {

        return query(sql) { it.copy() }.map { it.first() }.onErrorResumeNext {
            if (it is NoSuchElementException) {
                Single.error(NullPointerException())
            } else {
                Single.error(it)
            }
        }
    }

    override fun fetch(sql: String): Single<List<JsonObject>> {

        return query(sql) { it.copy() }
    }

    override fun <T : Any> fetch(sql: String, listOf: Class<T>): Single<List<T>> {

        return query(sql) { it.mapTo(listOf) }
    }

    override fun execute(sql: String): Completable {
        return vertx.rxExecuteBlocking<Boolean>({
            try {
                it.complete(connection.prepareStatement(sql).execute())
            } catch (e: Exception) {
                it.fail(e)
            }
        }, false)
            .ignoreElement()
    }

    private fun <T> query(sql: String, mapper: ((JsonObject) -> T)?): Single<List<T>> {

        return vertx.rxExecuteBlocking<ResultSet>({
            try {
                it.complete(connection.prepareStatement(sql).executeQuery())
            } catch (e: Exception) {
                it.fail(e)
            }
        }, false)
            .toSingle()
            .flatMap { rs ->
                val columns = (1..rs.metaData.columnCount).map { rs.metaData.getColumnName(it) }
                if (mapper != null) {
                    parseResultSet(columns, rs, mapper)
                } else {
                    closeResultSet(rs, Single.just(emptyList())) {
                        Single.error<List<T>>(it)
                    }
                }
            }
    }

    private fun JsonObject.putFromRs(key: String, rs: ResultSet) {

        with(rs.getObject(key)) {
            when (this) {
                is UUID -> put(key, toString())
                is Timestamp -> put(key, toInstant())
                is ClickHouseArray -> put(
                    key,
                    JsonArray(
                        ClickHouseArrayUtil.arrayToString(
                            array,
                            TimeZone.getDefault(),
                            TimeZone.getDefault()
                        ).let {
                            if (this@with.baseTypeName == "String"){
                                it.replace("'","\"")
                            } else {
                                it
                            }
                        }
                    )
                )
                else -> put(key, this)
            }
        }
    }

    private fun <T> parseResultSet(
        columns: List<String>,
        rs: ResultSet,
        mapper: (JsonObject) -> T,
        result: MutableList<T> = mutableListOf()
    ): Single<List<T>> {

        return vertx.rxExecuteBlocking<List<T>>({
            val json = JsonObject()
            val chunk = mutableListOf<T>()
            try {
                while (rs.next()) {
                    columns.forEach {
                        json.putFromRs(it, rs)
                    }
                    chunk.add(mapper(json))
                    json.clear()
                    if (chunk.size == MAPPING_SIZE) {
                        break
                    }
                }
            } catch (e: Exception) {
                it.fail(e)
            }
            it.complete(chunk)
        }, false).flatMapSingle { chunk ->
            if (chunk.size == MAPPING_SIZE) {
                parseResultSet(columns, rs, mapper, result.apply { addAll(chunk) })
            } else {
                closeResultSet(rs, Single.just(result.apply { addAll(chunk) })) {
                    Single.error(it)
                }
            }
        }
    }

    private fun <T> closeResultSet(rs: ResultSet, res: T, onError: (Throwable) -> T): T {

        return try {
            if (!rs.isClosed) {
                rs.close()
            }
            res
        } catch (e: Exception) {
            onError(e)
        }
    }
}
