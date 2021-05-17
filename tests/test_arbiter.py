import pytest

from time import sleep
from arbiter import Arbiter, Task
from tests.minion import stop_minion, start_minion

arbiter_host = "localhost"
arbiter_user = "user"
arbiter_password = "password"
arbiter_queue = "default"


class TestArbiter:
    p = None

    @classmethod
    def setup_class(cls):
        cls.p = start_minion()

    @classmethod
    def teardown_class(cls):
        stop_minion(cls.p)

    @staticmethod
    def test_task_in_task():
        arbiter = Arbiter(host=arbiter_host, port=5672, user=arbiter_user, password=arbiter_password)
        assert arbiter.workers()[arbiter_queue]['total'] == 10
        task_keys = []
        for _ in range(20):
            task_keys.append(arbiter.apply("simple_add", task_args=[1, 2])[0])
        for task_key in task_keys:
            assert arbiter.status(task_key)['state'] == 'initiated'
        for message in arbiter.wait_for_tasks(task_keys):
            assert message['state'] == 'done'
            assert message['result'] == 3
        for task_key in task_keys:
            assert arbiter.status(task_key)['state'] == 'done'
            assert arbiter.status(task_key)['result'] == 3
        arbiter.close()

    @staticmethod
    def test_squad():
        tasks_in_squad = 3
        arbiter = Arbiter(host=arbiter_host, port=5672, user=arbiter_user, password=arbiter_password)
        tasks = []
        for _ in range(tasks_in_squad):
            tasks.append(Task("simple_add", task_args=[1, 2]))
        squad_id = arbiter.squad(tasks)
        while arbiter.status(squad_id).get("state") != "done":
            sleep(1)
        status = arbiter.status(squad_id)
        assert status["done"] == tasks_in_squad
        assert len(status["tasks"]) == tasks_in_squad
        arbiter.close()

    @staticmethod
    def test_pipe():
        tasks_in_pipe = 20
        arbiter = Arbiter(host=arbiter_host, port=5672, user=arbiter_user, password=arbiter_password)
        tasks = []
        for _ in range(tasks_in_pipe):
            tasks.append(Task("add_in_pipe", task_args=[2]))
        pipe_id = None
        _loop_result = 0
        _loop_id = 1
        for message in arbiter.pipe(tasks, persistent_args=[2]):
            if "pipe_id" in message:
                pipe_id = message["pipe_id"]
            else:
                _loop_result = message['result']
                assert _loop_result == 4 * _loop_id
                _loop_id += 1
        status = arbiter.status(pipe_id)
        assert status["done"] == tasks_in_pipe
        assert len(status["tasks"]) == tasks_in_pipe
        arbiter.close()