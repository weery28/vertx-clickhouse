package me.coweery.vertx.clickhouse

import io.reactivex.Completable
import io.reactivex.Single
import io.vertx.core.json.JsonObject

interface ClickhouseClient {

    fun <T : Any> fetchOne(sql: String, resultClass: Class<T>): Single<T>

    fun fetchOne(sql: String): Single<JsonObject>

    fun fetch(sql: String): Single<List<JsonObject>>

    fun <T : Any> fetch(sql: String, listOf: Class<T>): Single<List<T>>

    fun execute(sql: String): Completable
}