package com.tm.vision.activation.api

import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.tm.vision.activation.api")

fun main() {
    val config = ApiConfig.fromEnv()
    val component = DaggerActivationApiComponent.factory().create(config)
    val vertx = component.vertx()
    val verticle = component.apiVerticle()

    runBlocking { vertx.deployVerticle(verticle).coAwait() }
    log.info("activation-api listening on port {}", verticle.server.actualPort())

    Runtime.getRuntime().addShutdownHook(
        Thread { runBlocking { vertx.close().coAwait() } },
    )
}
