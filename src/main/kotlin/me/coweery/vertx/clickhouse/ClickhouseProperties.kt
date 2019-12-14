package me.coweery.vertx.clickhouse

data class ClickhouseProperties(
    val host: String,
    val port: Int,
    val database: String,
    val username: String,
    val password: String
)