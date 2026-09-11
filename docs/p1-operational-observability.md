# P1 health and request logging

Implemented on the P1 branch. No production deployment, data rewrite, or new runtime dependency is part of this change.

## Readiness endpoint

`GET /health` (also HEAD) returns JSON with `Cache-Control: no-store` and a server-generated `X-Request-ID`.

- HTTP 200: a fresh read-only database connection can query the latest build metadata and find records in the athlete directory, activity, and coverage relations. The build must have succeeded and include a version and completion time. The response reports the dataset version, UTC build time, and P1 calculation-policy version.
- HTTP 503: the probe cannot read the database, metadata is missing/unsuccessful, or one of those core relations is empty. The response reports only `status: unavailable` and `component: snapshot`; no database path, query, or exception message is exposed.
- Other HTTP methods return 405.

The probe bypasses repository caches and uses bounded existence queries. It does **not** hash the 70 MB artifact or run the 19-check validator on every request. Those deeper checks remain in CI and `make check`. Readiness does not establish analytical completeness or statistical confidence.

Startup still fails fast if the configured database file is absent when the application imports; in that case no application endpoint can respond. The hosting process must detect that startup failure. The 503 response covers failures while an application process is running, not a promise to recover a missing deployment artifact automatically.

```sh
curl --fail --silent --show-error -i http://127.0.0.1:8000/health
```

## Structured request events

The `enduranceviz.requests` logger emits one JSON event to stderr per HTTP request, with:

- Event name and UTC start timestamp.
- A random server-generated request ID, also returned in the response header. Caller-supplied IDs are not trusted or copied.
- An allowlisted HTTP method and registered route pattern, such as `/athlete/{athlete_id}`. Unmatched paths use `<unmatched>`.
- Response status, duration in milliseconds, application body bytes, and whether the final response body was sent.
- Exception class name when an exception occurred, without its message or traceback.

No raw URLs, query/filter values, athlete IDs/names, activity text, request/response contents, IP addresses, cookies, authorization headers, user agents, or arbitrary caller IDs enter these events. Unknown HTTP methods are logged as `OTHER`. Logger propagation is disabled to avoid duplicate application events. Response byte counts describe ASGI body messages, not compressed network transfer size; timing measures application execution through body delivery to the server, not browser rendering or end-user latency.

Unhandled exceptions before response headers produce a generic HTTP 500 JSON response with the request ID and `no-store`. Exceptions after a response has started are logged and re-raised without attempting to replace the stream; cancellation also propagates. Logs retain the actual started status and completion state, or a null status if no response started. A 200 with an exception/incomplete body must therefore not be interpreted as a successful completed request.

`make run` disables Uvicorn's separate raw access log. For a manually launched preview, add `--no-access-log` to the Uvicorn command as well. Platform/proxy logs and server-level tracebacks are separate systems; this module does not claim to control Vercel retention, sanitize all infrastructure logs, or configure external monitoring/alerts. No such external services were added.

## Verification

Twelve regression tests cover normal responses, request-ID correlation/uniqueness, route-template redaction, query/header/cookie/body exclusion, unmatched routes, 405 responses, generic 500 handling, 200/503 health responses, HEAD behavior, uncached probes, failed/empty/missing database states, partial streams, cancellation, and non-HTTP pass-through. The full local suite passes 91 tests, followed by 19 artifact checks. A live local HTTP probe returned 200, `no-store`, the request ID, and the expected snapshot/policy metadata. The packaged database checksum is unchanged.
