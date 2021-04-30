from lithops_workerpool.pool import WorkerPool


def map_func(x):
    return x * 10


worker_pool = WorkerPool(workers=2,
                         runtime='aitorarjona/lithops-workerpool:0.1',
                         runtime_memory=256,
                         runtime_timeout=30,
                         wait_workers=True)
async_result = worker_pool.apply_map(map_function=map_func, map_iterdata=[1, 2, 3, 4])
result = async_result.result()
print(result)
worker_pool.close()

