package com.tm.vision.activation.api

/** Cấu hình activation-api, đọc từ biến môi trường. */
data class ApiConfig(
    val port: Int,
) {
    companion object {
        fun fromEnv(env: Map<String, String> = System.getenv()): ApiConfig =
            ApiConfig(
                port = env["ACTIVATION_API_PORT"]?.toInt() ?: 8080,
            )
    }
}
