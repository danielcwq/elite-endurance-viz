import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

import main
from enduranceviz.operations import LOGGER_NAME, RequestLogMiddleware
from enduranceviz.serving import ServingRepository


async def ok(request):
    return JSONResponse({'ok': True})


async def fail(request):
    raise RuntimeError('secret exception: athlete Alice /private/database.duckdb')


def test_app():
    return Starlette(routes=[Route('/person/{person_id}', ok), Route('/fail', fail)],
                     middleware=[Middleware(RequestLogMiddleware)])


class OperationalHTTPTests(unittest.TestCase):
    def test_logs_only_operational_fields_and_uses_server_request_id(self):
        with self.assertLogs(LOGGER_NAME, level='INFO') as logs, TestClient(test_app()) as client:
            response = client.get('/person/private-identity?q=private-query', headers={
                'x-request-id': 'private-caller-id', 'authorization': 'Bearer private-token',
                'cookie': 'private-cookie', 'user-agent': 'private-agent'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(logs.records), 1)
        event = json.loads(logs.records[0].getMessage())
        self.assertEqual(set(event), {'event', 'timestamp_utc', 'request_id', 'method', 'route',
                                     'status', 'duration_ms', 'body_bytes', 'response_complete'})
        self.assertEqual(event['route'], '/person/{person_id}')
        self.assertEqual(event['request_id'], response.headers['x-request-id'])
        self.assertRegex(event['request_id'], r'^[0-9a-f]{32}$')
        self.assertNotIn('private-', logs.records[0].getMessage())
        self.assertGreaterEqual(event['duration_ms'], 0)
        self.assertEqual(event['body_bytes'], len(response.content))
        self.assertTrue(event['response_complete'])

    def test_unmatched_path_and_method_failures_do_not_leak_input(self):
        with self.assertLogs(LOGGER_NAME, level='INFO') as logs, TestClient(test_app()) as client:
            self.assertEqual(client.get('/private-name/private-value').status_code, 404)
            self.assertEqual(client.post('/person/private-id', content='private-body').status_code, 405)
        events = [json.loads(record.getMessage()) for record in logs.records]
        self.assertEqual([(e['route'], e['status']) for e in events], [('<unmatched>', 404), ('/person/{person_id}', 405)])
        self.assertNotIn('private-', str(logs.output))

    def test_exception_returns_generic_500_and_logs_type_not_message(self):
        with self.assertLogs(LOGGER_NAME, level='ERROR') as logs, TestClient(test_app()) as client:
            response = client.get('/fail')
        event = json.loads(logs.records[0].getMessage())
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json(), {'error': 'Internal server error', 'request_id': event['request_id']})
        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertEqual(event['error_type'], 'RuntimeError')
        self.assertEqual(event['status'], 500)
        self.assertNotIn('Alice', response.text + str(logs.output))
        self.assertNotIn('/private', response.text + str(logs.output))

    def test_request_ids_are_distinct_and_body_is_not_logged(self):
        with self.assertLogs(LOGGER_NAME, level='INFO') as logs, TestClient(test_app()) as client:
            first = client.get('/person/one')
            second = client.get('/person/two')
        self.assertNotEqual(first.headers['x-request-id'], second.headers['x-request-id'])
        self.assertTrue(all('"ok"' not in record.getMessage() for record in logs.records))

    def test_health_exposes_only_version_and_policy_with_no_cache(self):
        with self.assertLogs(LOGGER_NAME, level='INFO') as logs, TestClient(main.app) as client:
            response = client.get('/health')
            head = client.head('/health')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(head.status_code, 200)
        self.assertEqual(head.content, b'')
        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertEqual(set(response.json()), {'status', 'dataset_version', 'dataset_built_at_utc', 'analytics_policy'})
        self.assertEqual(response.json()['status'], 'ok')
        self.assertTrue(response.json()['dataset_built_at_utc'].endswith('+00:00'))
        self.assertEqual(json.loads(logs.records[0].getMessage())['route'], '/health')

    def test_health_failure_is_503_without_database_path_or_exception(self):
        with patch.object(main.repository, 'health_metadata', side_effect=duckdb.IOException('/private/missing.duckdb')), self.assertLogs(LOGGER_NAME, level='ERROR') as logs, TestClient(main.app) as client:
            response = client.get('/health')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {'status': 'unavailable', 'component': 'snapshot'})
        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertNotIn('/private', response.text + str(logs.output))
        self.assertEqual(json.loads(logs.records[0].getMessage())['status'], 503)

    def test_missing_build_metadata_is_unavailable(self):
        with patch.object(main.repository, 'health_metadata', return_value=None), self.assertLogs(LOGGER_NAME, level='ERROR'), TestClient(main.app) as client:
            self.assertEqual(client.get('/health').status_code, 503)

    def test_health_is_get_head_only(self):
        with self.assertLogs(LOGGER_NAME, level='INFO'), TestClient(main.app) as client:
            self.assertEqual(client.post('/health').status_code, 405)

    def test_repository_probe_is_not_cached_and_rejects_failed_build(self):
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / 'health.duckdb'
            with duckdb.connect(str(database)) as c:
                c.execute('''
                    CREATE TABLE dataset_builds AS SELECT 'fixture' AS specification_version,
                        now() AS started_at_utc, now() AS completed_at_utc, 'succeeded' AS status;
                    CREATE TABLE athlete_directory_2024 AS SELECT 1 AS n;
                    CREATE TABLE activities_2024 AS SELECT 1 AS n;
                    CREATE TABLE data_coverage_2024 AS SELECT 1 AS n;
                ''')
            repository = ServingRepository(database)
            self.assertEqual(repository.health_metadata()['dataset_version'], 'fixture')
            with duckdb.connect(str(database)) as c:
                c.execute("UPDATE dataset_builds SET status='failed'")
            self.assertIsNone(repository.health_metadata())
            with duckdb.connect(str(database)) as c:
                c.execute("UPDATE dataset_builds SET status='succeeded'")
                c.execute('DELETE FROM activities_2024')
            self.assertIsNone(repository.health_metadata())
            with duckdb.connect(str(database)) as c:
                c.execute('DROP TABLE data_coverage_2024')
            with self.assertRaises(duckdb.Error):
                repository.health_metadata()


class OperationalASGITests(unittest.IsolatedAsyncioTestCase):
    async def test_stream_failure_preserves_started_status_and_reraises(self):
        messages = []

        async def stream(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'partial', 'more_body': True})
            raise RuntimeError('private-stream-detail')

        async def send(message):
            messages.append(message)

        with self.assertLogs(LOGGER_NAME, level='ERROR') as logs, self.assertRaises(RuntimeError):
            await RequestLogMiddleware(stream)({'type': 'http', 'method': 'GET'}, None, send)
        event = json.loads(logs.records[0].getMessage())
        self.assertEqual(event['status'], 200)
        self.assertFalse(event['response_complete'])
        self.assertEqual(event['body_bytes'], 7)
        self.assertEqual(len(messages), 2)
        self.assertNotIn('private-stream-detail', str(logs.output))

    async def test_cancellation_is_not_swallowed_or_reported_as_sent_500(self):
        async def canceled(scope, receive, send):
            raise asyncio.CancelledError()

        with self.assertLogs(LOGGER_NAME, level='ERROR') as logs, self.assertRaises(asyncio.CancelledError):
            await RequestLogMiddleware(canceled)({'type': 'http', 'method': 'private-method'}, None, None)
        event = json.loads(logs.records[0].getMessage())
        self.assertEqual(event['method'], 'OTHER')
        self.assertIsNone(event['status'])
        self.assertFalse(event['response_complete'])

    async def test_non_http_scopes_pass_through_without_log(self):
        seen = []

        async def passthrough(scope, receive, send):
            seen.append(scope['type'])

        with self.assertNoLogs(LOGGER_NAME):
            await RequestLogMiddleware(passthrough)({'type': 'lifespan'}, None, None)
            await RequestLogMiddleware(passthrough)({'type': 'websocket'}, None, None)
        self.assertEqual(seen, ['lifespan', 'websocket'])


if __name__ == '__main__':
    unittest.main()
