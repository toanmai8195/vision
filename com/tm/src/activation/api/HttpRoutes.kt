package com.tm.vision.activation.api

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import javax.inject.Inject
import javax.inject.Singleton

/** Khai báo route. Handler không chứa business logic (CLAUDE.md §11). */
@Singleton
class HttpRoutes @Inject constructor(
    private val vertx: Vertx,
    private val registry: PrometheusMeterRegistry,
) {
    fun router(): Router {
        val router = Router.router(vertx)
        router.get("/healthz").handler { ctx -> ctx.response().end("ok") }
        router.get("/metrics").handler { ctx ->
            ctx.response()
                .putHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                .end(registry.scrape())
        }
        // TODO(P6): /v1/segments/{id}/count, /users, /contains; /v1/users/{id}/segments; exports.
        return router
    }
}
