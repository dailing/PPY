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
        self._shared_property = {}
        logger.info('init')
        for i in self.__dir__():
            ti = type(self.__getattribute__(i))
            if issubclass(ti, SharedProperty):
                logger.info(f'initiate SharedProperty: {i}')
                sp = self.__getattribute__(i)
                sp._init(self, i, redis_pool)
                self._shared_property[i] = sp

    def __del__(self):
        logger.info('destroy class')
        for name, sp in self._shared_property.items():
            sp._del(self, name)


class SharedProperty:
    def __init__(self):
        self.attr_name = None
        self.value_key = None
        self.reference_count_key = None

    def _handle_init_get(func):
        def wrapper(self, instance, owner):
            if self.value_key is None or not hasattr(instance, self.value_key):
                return self
            return func(self, instance, owner)

        return wrapper

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

    def _init(self, instance: SharedClass, attr_name, redis_pool=None):
        if redis_pool is None:
            self._redis = redis.Redis(host='localhost', port=6379, db=0)
        else:
            self._redis = redis.Redis(connection_pool=redis_pool)
        self.attr_name = attr_name
        self.value_key = f'__{self.attr_name}_redis_key__.value'
        self.reference_count_key = f'__{self.attr_name}_redis_key__.ref_count'
        setattr(
            instance,
            self.value_key,
            f'{instance._instance_id}.{attr_name}.value')
        setattr(
            instance,
            self.reference_count_key,
            f'{instance._instance_id}.{attr_name}.reference_count'
        )
        self._redis.incr(getattr(instance, self.reference_count_key), 1)

    @_handle_init_get
    def __get__(self, instance: SharedClass, owner):
        logger.info(f'get property:{self.attr_name}')
        return self._get_sync_val_(
            getattr(instance, self.value_key))

    def __set__(self, instance, value):
        logger.info(f'set property {self.attr_name} to: {value}')
        self._set_sync_val_(
            getattr(instance, self.value_key),
            value)

    def _del(self, instance: SharedClass, attr_name):
        if self.reference_count_key is None:
            return
        ref = getattr(instance, self.reference_count_key)
        val = getattr(instance, self.value_key)
        refc = self._redis.decr(ref, 1)
        if refc == 0:
            logger.info(f'destroy instance {self.attr_name}')
            self._redis.delete(ref, val)


class RemoteEnv:
    pass
