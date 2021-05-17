import pytest

from arbiter import RPCClient
from tests.minion import stop_minion, start_minion

arbiter_host = "localhost"
arbiter_user = "user"
arbiter_password = "password"
arbiter_queue = "default"


class TestJSONRCP:
    p = None

    @classmethod
    def setup_class(cls):
        cls.p = start_minion(rpc=True)

    @classmethod
    def teardown_class(cls):
        stop_minion(cls.p)

    @staticmethod
    def test_simple_task_args():
        rpc = RPCClient(arbiter_host, port=5672, user=arbiter_user, password=arbiter_password)
        assert rpc.call(arbiter_queue, 'simple_add', (1, 7,)) == 8

    @staticmethod
    def test_simple_task_kwargs():
        rpc = RPCClient(arbiter_host, port=5672, user=arbiter_user, password=arbiter_password)
        assert rpc.call(arbiter_queue, 'simple_add', (), {"x": 1, "y": 7}) == 8

    @staticmethod
    def test_simple_task_mix():
        rpc = RPCClient(arbiter_host, port=5672, user=arbiter_user, password=arbiter_password)
        assert rpc.call(arbiter_queue, 'simple_add', (1,), {"y": 7}) == 8
