from random import randint
import json
import time
from locust import User, events, TaskSet, task, constant
import redis
import gevent.monkey
import inspect

gevent.monkey.patch_all()


def load_config(file_path):
    config: any;
    """For loading the connection details of Redis"""
    with open(file_path) as property_file:
        configs = json.load(property_file)
    return configs


file_name_redis_conf = "redis_config.json"
configs = load_config(file_name_redis_conf)


class RedisClient(object):
    def __init__(self, host=configs["redis_host"], port=configs["redis_port"], password=configs["redis_password"]):
        print("redis_host: ", host)
        print("redis_port: ", port)
        print("redis_password: ", password)
        self.rc = redis.StrictRedis(host=host, port=port, password=password)

    def query(self, key, command='GET'):
        """Function to Test GET operation on Redis"""
        result = None
        start_time = time.time()
        previous_frame = inspect.currentframe().f_back
        _, _, task_name, _, _ = inspect.getframeinfo(previous_frame)

        try:
            result = self.rc.get(key)
            if not result:
                result = ''
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="grpc",
                name=task_name,
                response_time=total_time,
                response_length=0,
                exception=e
            )
        else:
            total_time = int((time.time() - start_time) * 1000)
            length = len(result)
            events.request.fire(
                request_type="grpc",
                name=task_name,
                response_time=total_time,
                response_length=0,
            )
        return result

    def write(self, key, value, command='SET'):
        """Function to Test SET operation on Redis"""
        result = None
        start_time = time.time()
        previous_frame = inspect.currentframe().f_back
        _, _, task_name, _, _ = inspect.getframeinfo(previous_frame)
        try:
            result = self.rc.set(key, value)
            if not result:
                result = ''
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="grpc",
                name=task_name,
                response_time=total_time,
                response_length=0,
                exception=e
            )
        else:
            total_time = int((time.time() - start_time) * 1000)
            length = 1
            events.request.fire(
                request_type="grpc",
                name=task_name,
                response_time=total_time,
                response_length=0,
            )
        return result


class RedisLocust(User):
    wait_time = constant(0.1)
    key_range = 500

    def __init__(self, *args, **kwargs):
        super(RedisLocust, self).__init__(*args, **kwargs)
        self.client = RedisClient()
        self.key = 'key1'
        self.value = 'value1'

    @task(2)
    def get_time(self):
        for i in range(self.key_range):
            self.key = 'key' + str(i)
            self.client.query(self.key)

    @task(1)
    def write(self):
        for i in range(self.key_range):
            self.key = 'key' + str(i)
            self.value = 'value' + str(i)
            self.client.write(self.key, self.value)

    @task(1)
    def get_key(self):
        var = str(randint(1, self.key_range - 1))
        self.key = 'key' + var
        self.value = 'value' + var
