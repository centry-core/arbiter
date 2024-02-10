## Testing app for arbiter

Launch redis container
```
docker run -d --rm --hostname arbiter-redis --name arbiter-redis \
           -p 6379:6379 redis:alpine redis-server
```

Launch minion app with `python minion.py`

## Running tests

to run tests you need to execution `python -q tests/`

## Test coverage

to check test coverage you need to run
```
coverage run --source=arbiter -m pytest -q tests/
coverage report -m
coverage html
```
