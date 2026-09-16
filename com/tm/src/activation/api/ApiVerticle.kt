package com.tm.vision.activation.api

import io.vertx.core.http.HttpServer
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.coAwait
import javax.inject.Inject

/** Verticle HTTP của activation-api. */
class ApiVerticle @Inject constructor(
    private val apiConfig: ApiConfig,
    private val routes: HttpRoutes,
) : CoroutineVerticle() {
    lateinit var server: HttpServer
        private set

    override suspend fun start() {
        server = vertx.createHttpServer()
            .requestHandler(routes.router())
            .listen(apiConfig.port)
            .coAwait()
    }
}
