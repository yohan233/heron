package(default_visibility = ["//visibility:public"])

load("/tools/rules/pex_rules", "pex_library", "pex_binary")

pex_library(
    name = "heron-python-py",
    srcs = glob(["**/*.py"]),
    deps = [
        "//heron/proto:proto-py",
    ],
)

# for egg production
pex_binary(
    name = "heron-python-package",
    srcs = glob(["**/*.py"]),
    deps = [
        ":heron-python-py",
    ]
)
