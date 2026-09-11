"""Minimal HTTP observability without request URLs, identities, or payloads."""

import json
import logging
from datetime import datetime, timezone
from time import perf_counter
from uuid import uuid4

from starlette.responses import JSONResponse


LOGGER_NAME = 'enduranceviz.requests'
logger = logging.getLogger(LOGGER_NAME)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


def route_pattern(scope):
    """Use the registered template; unmatched paths must never become log data."""
    endpoint = scope.get('endpoint')
    if endpoint is not None:
        for route in getattr(scope.get('app'), 'routes', ()):
            if getattr(route, 'endpoint', None) is endpoint:
                return route.path
    return '<unmatched>'


class RequestLogMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        started = perf_counter()
        timestamp = datetime.now(timezone.utc).isoformat()
        request_id = uuid4().hex
        scope.setdefault('state', {})['request_id'] = request_id
        status, body_bytes, response_started, complete = None, 0, False, False
        error_type = None

        async def tracked_send(message):
            nonlocal status, body_bytes, response_started, complete
            if message['type'] == 'http.response.start':
                headers = [(key, value) for key, value in message.get('headers', [])
                           if key.lower() != b'x-request-id']
                headers.append((b'x-request-id', request_id.encode('ascii')))
                message = {**message, 'headers': headers}
                status = message['status']
                response_started = True
            await send(message)
            if message['type'] == 'http.response.body':
                body_bytes += len(message.get('body', b''))
                complete = not message.get('more_body', False)

        try:
            await self.app(scope, receive, tracked_send)
        except BaseException as exc:
            error_type = type(exc).__name__
            if isinstance(exc, Exception) and not response_started:
                # A generic response keeps SQL, paths, and exception messages
                # out of both the response and our operational event.
                response = JSONResponse(
                    {'error': 'Internal server error', 'request_id': request_id},
                    status_code=500, headers={'Cache-Control': 'no-store'},
                )
                await response(scope, receive, tracked_send)
            else:
                # Do not replace an already-started streaming response or swallow
                # task cancellation. Log its actual status and completion state.
                raise
        finally:
            method = scope.get('method', '')
            event = {
                'event': 'http_request', 'timestamp_utc': timestamp,
                'request_id': request_id,
                'method': method if method in {'GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'TRACE', 'CONNECT'} else 'OTHER',
                'route': route_pattern(scope), 'status': status,
                'duration_ms': round((perf_counter() - started) * 1000, 3),
                'body_bytes': body_bytes, 'response_complete': complete,
            }
            if error_type:
                event['error_type'] = error_type
            level = logging.ERROR if error_type or (status is not None and status >= 500) else logging.INFO
            logger.log(level, json.dumps(event, separators=(',', ':')))
