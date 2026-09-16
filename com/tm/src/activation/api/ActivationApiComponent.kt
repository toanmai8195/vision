package com.tm.vision.activation.api

import dagger.BindsInstance
import dagger.Component
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.vertx.core.Vertx
import javax.inject.Singleton

/** Dagger component duy nhất của deployable activation-api. */
@Singleton
@Component(modules = [CoreModule::class])
interface ActivationApiComponent {
    fun vertx(): Vertx

    fun meterRegistry(): PrometheusMeterRegistry

    fun apiVerticle(): ApiVerticle

    @Component.Factory
    interface Factory {
        fun create(@BindsInstance config: ApiConfig): ActivationApiComponent
    }
}
