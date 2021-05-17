import pytest

from time import sleep, time
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
            assert arbiter.status(task_key)['state'] in ('initiated', 'running')
        for message in arbiter.wait_for_tasks(task_keys):
            assert message['state'] == 'done'
            assert message['result'] == 3
        for task_key in task_keys:
            assert arbiter.status(task_key)['state'] == 'done'
            assert arbiter.status(task_key)['result'] == 3
        assert arbiter.workers()[arbiter_queue]['available'] == 10
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
        assert arbiter.workers()[arbiter_queue]['available'] == 10
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
        assert arbiter.workers()[arbiter_queue]['available'] == 10
        arbiter.close()

    @staticmethod
    def test_kill_task():
        arbiter = Arbiter(host=arbiter_host, port=5672, user=arbiter_user, password=arbiter_password)
        start = time()
        tasks = arbiter.apply("long_running")
        for task_key in tasks:
            assert arbiter.status(task_key)['state'] == 'initiated'
        sleep(2)  # time for task to settle
        arbiter.kill(tasks[0], sync=True)
        for message in arbiter.wait_for_tasks(tasks):
            assert message['state'] == 'done'
            assert time()-start < 180 # 180 sec is a length of task
        assert arbiter.workers()[arbiter_queue]['available'] == 10
        arbiter.close()

    @staticmethod
    def test_kill_group():
        tasks_in_squad = 3
        start = time()
        arbiter = Arbiter(host=arbiter_host, port=5672, user=arbiter_user, password=arbiter_password)
        tasks = []
        for _ in range(tasks_in_squad):
            tasks.append(Task("long_running"))
        squad_id = arbiter.squad(tasks)
        sleep(5)  # time for squad to settle
        arbiter.kill_group(squad_id)
        while arbiter.status(squad_id).get("state") != "done":
            sleep(1)
        assert time() - start < 180
        assert arbiter.workers()[arbiter_queue]['available'] == 10
        arbiter.close()

    @staticmethod
    def test_squad_callback():
        tasks_in_squad = 3
        arbiter = Arbiter(host=arbiter_host, port=5672, user=arbiter_user, password=arbiter_password)
        tasks = []
        for _ in range(tasks_in_squad):
            tasks.append(Task("simple_add", task_args=[1, 2]))
        squad_id = arbiter.squad(tasks, callback=Task("simple_add", task_args=[5, 4]))
        while arbiter.status(squad_id).get("state") != "done":
            sleep(1)
        status = arbiter.status(squad_id)
        assert status["done"] == tasks_in_squad + 1
        assert len(status["tasks"]) == tasks_in_squad + 1
        assert status["tasks"][-1]['task_type'] == "callback"
        assert status["tasks"][-1]['result'] == 9
        assert arbiter.workers()[arbiter_queue]['available'] == 10
        arbiter.close()

    @staticmethod
    def test_squad_finalyzer():
        tasks_in_squad = 3
        arbiter = Arbiter(host=arbiter_host, port=5672, user=arbiter_user, password=arbiter_password)
        tasks = []
        for _ in range(tasks_in_squad):
            tasks.append(Task("simple_add", task_args=[1, 2]))
        tasks.append(Task("simple_add", task_args=[5, 5], task_type='finalize'))
        squad_id = arbiter.squad(tasks, callback=Task("simple_add", task_args=[5, 4]))
        while arbiter.status(squad_id).get("state") != "done":
            sleep(1)
        status = arbiter.status(squad_id)
        assert status["done"] == tasks_in_squad + 2  # callback + finalizer
        assert len(status["tasks"]) == tasks_in_squad + 2  # callback + finalizer
        assert status["tasks"][-1]['task_type'] == "finalize"
        assert status["tasks"][-1]['result'] == 10
        assert arbiter.workers()[arbiter_queue]['available'] == 10
        arbiter.close()

    @staticmethod
    def test_sync_task():
        arbiter = Arbiter(host=arbiter_host, port=5672, user=arbiter_user, password=arbiter_password)
        for result in arbiter.add_task(Task("simple_add", task_args=[1, 2]), sync=True):
            if isinstance(result, dict):
                assert result['state'] == 'done'
                assert result['result'] == 3
        assert arbiter.workers()[arbiter_queue]['available'] == 10
