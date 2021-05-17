## Testing app for arbiter

Launch rabbitmq container
```
docker run -d --rm --hostname arbiter-rabbit --name arbiter-rabbit \
           -p 5672:5672 -p 15672:15672 -e RABBITMQ_DEFAULT_USER=user \
           -e RABBITMQ_DEFAULT_PASS=password \
           -e RABBITMQ_DEFAULT_VHOST=carrier \
           rabbitmq:3-management
```

Launch minion app with `python minion.py`

Access rabbit management console through `http://localhost:15672`

go to Queues and select `default`

click on `publish message` and post following message to body

```json
{
    "type": "task",
    "task_name": "add",
    "task_key": "2",
    "args": [1,2]
}
```

in the logs of running minion.py you need to see a record that task was executed and result published

## Running tests

to run tests you need to execution `python -q tests/`