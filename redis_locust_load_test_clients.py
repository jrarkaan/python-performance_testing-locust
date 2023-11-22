import random
import json
import time
from locust import User, events, TaskSet, task, constant, tag
import redis
import gevent.monkey
import inspect

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

    def get_query_string(self, key, command='GET'):
        result = None
        start_time = time.time()
        try:
            result = self.rc.get(key)
            if not result:
                result = ''
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type=command,
                name=key,
                response_time=total_time,
                exception=e,
            )
        else:
            total_time = int((time.time() - start_time) * 1000)
            length = len(str(result))
            events.request.fire(
                request_type=command,
                name=key,
                response_time=total_time,
                response_length=length,
            )
        return result

    def set_query_string(self, key, command='SET'):
        result = None
        bid_price = random.randint(47238, 57238)
        redis_response = {'bids': bid_price}
        start_time = time.time()
        try:
            result = self.rc.set(key, json.dumps(redis_response))
            if not result:
                result = ''
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type=command,
                name=key,
                response_time=total_time,
                exception=e,
            )
        else:
            total_time = int((time.time() - start_time) * 1000)
            length = len(str(result))
            events.request.fire(
                request_type=command,
                name=key,
                response_time=total_time,
                response_length=length,
            )
        return result

    def lpush_in_list(self, key, command='LPUSH'):
        result = None
        start_time = time.time()
        try:
            result = self.rc.lpush(key, 0, 0, 0, 0, 0)
            if not result:
                result = ''
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type=command,
                name=key,
                response_time=total_time,
                exception=e,
            )
        else:
            total_time = int((time.time() - start_time) * 1000)
            length = result.bit_length()
            events.request.fire(
                request_type=command,
                name=key,
                response_time=total_time,
                response_length=length,
            )

    def sadd_in_set(self, key, command='SADD'):
        result = None
        visitors = {"dan", "jon", "alex"}
        start_time = time.time()
        try:
            result = self.rc.sadd(key, *visitors)
            if not result:
                result = ''
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type=command,
                name=key,
                response_time=total_time,
                exception=e,
            )
        else:
            total_time = int((time.time() - start_time) * 1000)
            length = len(str(result))
            events.request.fire(
                request_type=command,
                name=key,
                response_time=total_time,
                response_length=length,
            )
        return result

class RedisLocust(User):
    def __init__(self, *args, **kwargs):
        super(RedisLocust, self).__init__(*args, **kwargs)
        self.client = RedisClient()

    @task
    @tag("string")
    def string_operations(self):
        self.client.set_query_string("string_set_operation")
        self.client.get_query_string("string_get_operation")

    @task
    @tag("list")
    def list_operation(self):
        self.client.lpush_in_list("list_lpush_operation")