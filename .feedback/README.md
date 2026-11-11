# Feedback task starting environment

Upstream: https://github.com/encode/httpcore
Upstream commit: `10a658221deb38a4c5b16db55ab554b0bf731707`
Prepared on macOS with CPython 3.12.11. No external database, queue, or cloud account is required.
The dependency file pins the installed runtime and test packages for the selected baseline. It is not a claim that all optional upstream test suites are installed.

From the repository root:

```sh
export NO_INTERNET=1 TZ=UTC UV_NO_CONFIG=1

uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r .feedback/requirements.txt -e .
.venv/bin/python -m pytest -q -o addopts= -o asyncio_default_fixture_loop_scope=function tests/_sync/test_connection_pool.py tests/_async/test_connection_pool.py
```

Scope note: The scope is the command below, not all optional upstream integrations.

Observed baseline (2026-11-11): `51 passed in 0.52s`
Whole-command wall time: 0.798 seconds, exit code 0. This is one local measurement, not a runtime guarantee. The first install is excluded.

This seed adds preparation metadata only. Application sources and upstream tests are unchanged; there is no task solution in this commit.
