import redis
import pickle
import uuid
from abc import ABC, abstractmethod, abstractproperty
from logs import get_logger

logger = get_logger('PPY')


class SharedClass(ABC):
    def __init__(self, redis_pool=None, instance_id=None):
        super().__init__()
        if instance_id is None:
            instance_id = uuid.uuid4().__str__()
        self._instance_id = instance_id
        if redis_pool is None:
            self._redis = redis.Redis(host='localhost', port=6379, db=0)
        else:
            self._redis = redis.Redis(connection_pool=redis_pool)
        logger.info('init')
        for i in self.__dir__():
            ti = type(self.__getattribute__(i))
            if issubclass(ti, SharedProperty):
                logger.info(f'initiate SharedProperty: {i}')
                sp = self.__getattribute__(i)
                sp._init(self, i)

    @staticmethod
    def _value_encoder(val):
        return pickle.dumps(val)

    @staticmethod
    def _value_decoder(bytes_content):
        return pickle.loads(bytes_content)

    def _set_sync_val_(self, name, value):
        value = self._value_encoder(value)
        self._redis.set(name, value)

    def _get_sync_val_(self, name):
        value = self._redis.get(name)
        return self._value_decoder(value)


class SharedProperty:
    def __init__(self):
        self.attr_name = None
        self.redis_key = None

    def _handle_init_get(func):
        def wrapper(self, instance, owner):
            if self.redis_key is None or not hasattr(instance, self.redis_key):
                return self
            return func(self, instance, owner)

        return wrapper

    def _init(self, instance: SharedClass, attr_name):
        self.attr_name = attr_name
        self.redis_key = f'__{self.attr_name}_redis_key__'
        setattr(
            instance,
            self.redis_key,
            f'{instance._instance_id}.{attr_name}')

    @_handle_init_get
    def __get__(self, instance: SharedClass, owner):
        logger.info(f'get property:{self.attr_name}')
        return instance._get_sync_val_(
            getattr(instance, self.redis_key))

    def __set__(self, instance, value):
        logger.info(f'set property {self.attr_name} to: {value}')
        instance._set_sync_val_(
            getattr(instance, self.redis_key),
            value)

    def __del__(self):
        pass


class RemoteEnv:
    pass
