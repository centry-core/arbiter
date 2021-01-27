from arbiter import RPCClient

arbiter_host = "localhost"
queue = "rcp_queue"

rpc = RPCClient(arbiter_host, port=5672, user='user', password='password')
print(rpc.call(queue, 'simple_add', [1, 7]))
rpc.disconnect()
