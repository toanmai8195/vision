package com.tm.vision.activation.api

import dagger.Module
import dagger.Provides
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.vertx.core.Vertx
import io.vertx.micrometer.MicrometerMetricsFactory
import javax.inject.Singleton

/** Vertx + registry Prometheus dùng chung toàn service. */
@Module
object CoreModule {
    @Provides
    @Singleton
    fun meterRegistry(): PrometheusMeterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    @Provides
    @Singleton
    fun vertx(registry: PrometheusMeterRegistry): Vertx =
        Vertx.builder()
            .withMetrics(MicrometerMetricsFactory(registry))
            .build()
}
