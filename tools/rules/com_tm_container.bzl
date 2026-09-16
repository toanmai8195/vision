"""Macro build binary + OCI image cho từng ngôn ngữ (theo pandora/raccoon).

Mỗi macro sinh cùng một bộ target:

    //path/to/app:<name>          binary chạy local (bazel run)
    //path/to/app:<name>_tar      layer ứng dụng
    //path/to/app:<name>_image    oci_image
    //path/to/app:<name>_docker   load image vào Docker local
    //path/to/app:<name>_push     push image (chỉ khi truyền `repository`)

Image tag: com.tm.{go,kt,py,airflow}.<name>:v1.0.0

Build image đúng kiến trúc máy chạy container:

    bazel run --config=linux-arm64 //com/tm/src/ingest/collector:event_collector_docker
"""

load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_java//java:defs.bzl", "java_binary")
load("@rules_kotlin//kotlin:jvm.bzl", "kt_jvm_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load", "oci_push")
load("@rules_python//python:defs.bzl", "py_binary")
load("@tar.bzl", "tar")

DEFAULT_IMAGE_TAG = "v1.0.0"

def _container_targets(
        name,
        base,
        repo_tag,
        tar_srcs,
        entrypoint = None,
        cmd = None,
        env = None,
        workdir = None,
        exposed_ports = [],
        repository = None,
        image_tag = DEFAULT_IMAGE_TAG,
        visibility = ["//visibility:public"]):
    """Đóng `tar_srcs` thành một layer rồi sinh image / docker load / push."""

    tar(
        name = name + "_tar",
        srcs = tar_srcs,
        out = name + "_layer.tar",
        visibility = visibility,
    )

    oci_image(
        name = name + "_image",
        base = base,
        cmd = cmd,
        entrypoint = entrypoint,
        env = env,
        exposed_ports = exposed_ports,
        tars = [":" + name + "_tar"],
        visibility = visibility,
        workdir = workdir,
    )

    oci_load(
        name = name + "_docker",
        image = ":" + name + "_image",
        repo_tags = ["%s:%s" % (repo_tag, image_tag)],
        visibility = visibility,
    )

    if repository:
        oci_push(
            name = name + "_push",
            image = ":" + name + "_image",
            remote_tags = [image_tag],
            repository = repository,
            visibility = visibility,
        )

# =============================================================================
# GO
# =============================================================================

def com_tm_go_image(
        name,
        package_name,
        embed,
        data = [],
        args = [],
        exposed_ports = [],
        env = None,
        repository = None,
        image_tag = DEFAULT_IMAGE_TAG,
        visibility = ["//visibility:public"]):
    """go_binary (static, CGO off) + OCI image trên alpine.

    Args:
        name: tên gốc cho mọi target.
        package_name: truyền `package_name()` từ BUILD file.
        embed: go_library chứa `package main` (thường do gazelle sinh).
        data: file runtime, cũng được bake vào image.
        args: tham số mặc định khi chạy local.
        exposed_ports: port expose trong container.
        env: biến môi trường cho container.
        repository: registry cho `<name>_push`; bỏ trống để không sinh target push.
        image_tag: tag image.
        visibility: visibility của target sinh ra.
    """

    go_binary(
        name = name,
        args = args,
        data = data,
        embed = embed,
        pure = "on",
        static = "on",
        visibility = visibility,
    )

    # rules_go đặt binary ở <name>_/<name>; copy ra path ổn định cho entrypoint.
    copy_file(
        name = name + "_bin",
        src = ":" + name,
        out = "bin/" + name,
        is_executable = True,
    )

    _container_targets(
        name = name,
        base = "@go_base",
        entrypoint = ["/%s/bin/%s" % (package_name, name)],
        env = env,
        exposed_ports = exposed_ports,
        image_tag = image_tag,
        repo_tag = "com.tm.go.%s" % name,
        repository = repository,
        tar_srcs = [":" + name + "_bin"] + data,
        visibility = visibility,
    )

# =============================================================================
# KOTLIN
# =============================================================================

def com_tm_kt_image(
        name,
        package_name,
        main_class,
        srcs,
        deps = [],
        runtime_deps = [],
        resources = [],
        data = [],
        jvm_flags = [],
        args = [],
        exposed_ports = [],
        env = None,
        repository = None,
        image_tag = DEFAULT_IMAGE_TAG,
        visibility = ["//visibility:public"]):
    """kt_jvm_library + java_binary (_deploy.jar) + OCI image trên Temurin JRE 21.

    Sinh thêm `<name>_lib` để test có thể depend.

    Args:
        name: tên gốc cho mọi target.
        package_name: truyền `package_name()` từ BUILD file.
        main_class: vd `com.tm.vision.activation.api.MainKt`.
        srcs: source Kotlin.
        deps: dependency compile (Dagger: `//third_party/dagger`).
        runtime_deps: dependency chỉ cần lúc chạy.
        resources: resource đóng vào jar.
        data: file runtime cho binary local.
        jvm_flags: JVM flag dùng cả local và trong image.
        args: tham số mặc định khi chạy local.
        exposed_ports: port expose trong container.
        env: biến môi trường cho container.
        repository: registry cho `<name>_push`.
        image_tag: tag image.
        visibility: visibility của target sinh ra.
    """

    lib_name = name + "_lib"

    kt_jvm_library(
        name = lib_name,
        srcs = srcs,
        resources = resources,
        visibility = visibility,
        deps = deps,
    )

    java_binary(
        name = name,
        args = args,
        data = data,
        jvm_flags = jvm_flags,
        main_class = main_class,
        runtime_deps = [":" + lib_name] + runtime_deps,
        visibility = visibility,
    )

    _container_targets(
        name = name,
        base = "@java_base",
        entrypoint = ["java"] + jvm_flags + ["-jar", "/%s/%s_deploy.jar" % (package_name, name)],
        env = env,
        exposed_ports = exposed_ports,
        image_tag = image_tag,
        repo_tag = "com.tm.kt.%s" % name,
        repository = repository,
        tar_srcs = [":" + name + "_deploy.jar"],
        visibility = visibility,
    )

# =============================================================================
# PYTHON
# =============================================================================

def com_tm_py_image(
        name,
        package_name,
        main,
        srcs,
        deps = [],
        data = [],
        args = [],
        requirements = None,
        exposed_ports = [],
        env = None,
        repository = None,
        image_tag = DEFAULT_IMAGE_TAG,
        visibility = ["//visibility:public"]):
    """py_binary + OCI image trên python:3.11-slim.

    `deps` (kể cả `@pypi//...`) dùng cho binary local. Trong image, source nằm ở
    path tương đối workspace (PYTHONPATH=/) nên import `com.tm...` hoạt động.

    TODO(verify): image chưa hermetic cho pip deps — hiện cài `requirements` lúc
    container start. Chuyển sang layer site-packages dựng sẵn khi service Python
    đầu tiên có dependency thật (P2).

    Args:
        name: tên gốc cho mọi target.
        package_name: truyền `package_name()` từ BUILD file.
        main: file main, vd "main.py".
        srcs: source Python của binary.
        deps: dependency Bazel.
        data: file runtime, cũng được bake vào image.
        args: tham số mặc định khi chạy local.
        requirements: requirements.txt cài trong container (tạm thời, xem TODO).
        exposed_ports: port expose trong container.
        env: biến môi trường cho container.
        repository: registry cho `<name>_push`.
        image_tag: tag image.
        visibility: visibility của target sinh ra.
    """

    py_binary(
        name = name,
        srcs = srcs,
        args = args,
        data = data,
        main = main,
        visibility = visibility,
        deps = deps,
    )

    main_path = "/%s/%s" % (package_name, main)

    if requirements:
        entrypoint = [
            "/bin/sh",
            "-c",
            "pip install -q --no-cache-dir -r /%s/%s && exec python3 %s" % (package_name, requirements, main_path),
        ]
    else:
        entrypoint = ["python3", main_path]

    image_env = {"PYTHONPATH": "/"}
    if env:
        image_env.update(env)

    _container_targets(
        name = name,
        base = "@python_base",
        entrypoint = entrypoint,
        env = image_env,
        exposed_ports = exposed_ports,
        image_tag = image_tag,
        repo_tag = "com.tm.py.%s" % name,
        repository = repository,
        tar_srcs = srcs + data + ([requirements] if requirements else []),
        visibility = visibility,
    )

# =============================================================================
# AIRFLOW
# =============================================================================

def com_tm_airflow_image(
        name,
        srcs,
        requirements = [],
        image_tag = DEFAULT_IMAGE_TAG,
        visibility = ["//visibility:public"]):
    """Bake DAG vào image Airflow (theo pandora).

    Args:
        name: tên gốc cho mọi target.
        srcs: file DAG.
        requirements: pip package thêm (qua `_PIP_ADDITIONAL_REQUIREMENTS`).
        image_tag: tag image.
        visibility: visibility của target sinh ra.
    """

    env = {"AIRFLOW__CORE__DAGS_FOLDER": "/com/tm/dags"}
    if requirements:
        env["_PIP_ADDITIONAL_REQUIREMENTS"] = " ".join(requirements)

    _container_targets(
        name = name,
        base = "@airflow_base",
        env = env,
        image_tag = image_tag,
        repo_tag = "com.tm.airflow.%s" % name,
        tar_srcs = srcs,
        visibility = visibility,
    )
