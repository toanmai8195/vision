"""Macro build binary + OCI image cho từng ngôn ngữ (theo thor).

Mỗi macro sinh cùng một bộ target:

    //path/to/app:<name>          binary chạy local (bazel run)
    //path/to/app:<name>_image    oci_image
    //path/to/app:<name>_docker   load image vào Docker local
    //path/to/app:<name>_push     push image (chỉ khi truyền `repository`)

Image tag: com.tm.{py,go}.<name>:v1.0.0. Kotlin/Airflow thêm ở các bước sau.

    bazel run --config=linux-arm64 //path/to/app:<name>_docker
"""

load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@rules_go//go:def.bzl", "go_binary")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load", "oci_push")
load("@rules_python//python:defs.bzl", "py_binary")
load("@tar.bzl", "tar")

DEFAULT_IMAGE_TAG = "v1.0.0"
_PY_ROOT = "/app"

def com_tm_py_image(
        name,
        main,
        srcs,
        deps = [],
        data = [],
        args = [],
        env = None,
        repository = None,
        image_tag = DEFAULT_IMAGE_TAG,
        visibility = ["//visibility:public"]):
    """py_binary + OCI image trên python:3.11-slim.

    Image chứa source `.py` tại /app (giữ nguyên đường dẫn package) và chạy
    bằng python của base image; chưa bake dependency pip.
    TODO(verify): chuyển sang layer site-packages dựng sẵn khi có service
    Python cần dependency (bước 4).

    Args:
        name: tên gốc cho mọi target.
        main: file .py chạy đầu tiên (trong `srcs`).
        srcs: source .py.
        deps: py_library / pip dep cho binary chạy local.
        data: file runtime, cũng được bake vào image.
        args: tham số mặc định khi chạy local.
        env: biến môi trường cho container.
        repository: registry cho `<name>_push`; bỏ trống để không sinh target push.
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

    tar(
        name = name + "_tar",
        srcs = srcs + data,
        out = name + "_layer.tar",
        mtree = [
            "./app/%s/%s uid=0 gid=0 mode=0755 type=file content=$(execpath %s)" % (
                native.package_name(),
                s.rsplit("/", 1)[-1] if ":" not in s else s.split(":")[-1],
                s,
            )
            for s in srcs + data
        ],
        visibility = visibility,
    )

    env_all = {"PYTHONPATH": _PY_ROOT, "PYTHONUNBUFFERED": "1"}
    if env:
        env_all.update(env)

    oci_image(
        name = name + "_image",
        base = "@python_base",
        entrypoint = ["python3", "%s/%s/%s" % (_PY_ROOT, native.package_name(), main)],
        env = env_all,
        tars = [":" + name + "_tar"],
        visibility = visibility,
        workdir = _PY_ROOT,
    )

    oci_load(
        name = name + "_docker",
        image = ":" + name + "_image",
        repo_tags = ["com.tm.py.%s:%s" % (name, image_tag)],
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
    """go_binary (static, CGO off) + OCI image trên distroless.

    Args:
        name: tên gốc cho mọi target.
        package_name: truyền `package_name()` từ BUILD file.
        embed: go_library chứa `package main`.
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

    tar(
        name = name + "_tar",
        srcs = [":" + name + "_bin"] + data,
        out = name + "_layer.tar",
        visibility = visibility,
    )

    oci_image(
        name = name + "_image",
        base = "@distroless_base",
        entrypoint = ["/%s/bin/%s" % (package_name, name)],
        env = env,
        exposed_ports = exposed_ports,
        tars = [":" + name + "_tar"],
        visibility = visibility,
    )

    oci_load(
        name = name + "_docker",
        image = ":" + name + "_image",
        repo_tags = ["com.tm.go.%s:%s" % (name, image_tag)],
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
