import unittest
from PPY import SharedClass, SharedProperty, SharedDict, SharedQueue


class TestSimplePropertySyncClass(SharedClass):
    property1 = SharedProperty()
    dict1 = SharedDict()
    queue1 = SharedQueue()

    def __init__(self, redis_pool=None, instance_id=None):
        super().__init__(redis_pool, instance_id)


class TestSimplePropertySync(unittest.TestCase):
    def setUp(self) -> None:
        self.c1 = TestSimplePropertySyncClass()
        self.c2 = TestSimplePropertySyncClass(instance_id=self.c1._instance_id)

    def test_setter_getter_self(self):
        val = '12341421'
        self.c1.property1 = val
        self.assertEqual(val, self.c1.property1, 'get from same property not consistence')
        self.assertEqual(val, self.c2.property1, 'get from same property not consistence')
        self.assertEqual(val, self.c1.property1, 'get from same property not consistence')
        self.assertEqual(val, self.c2.property1, 'get from same property not consistence')


class TestSharedDict(unittest.TestCase):
    def setUp(self) -> None:
        self.d1 = TestSimplePropertySyncClass()
        self.d2 = TestSimplePropertySyncClass(instance_id=self.d1._instance_id)

    def test_set_get_dict(self):
        test_val1 = 10
        self.d1.dict1[0] = test_val1
        self.assertEqual(self.d1.dict1[0], test_val1)
        self.assertEqual(self.d2.dict1[0], test_val1)
        self.assertRaises(KeyError, lambda: self.d1.dict1['0'])
        test_val2 = 'FUCK YOU'
        self.d2.dict1['0'] = test_val2
        self.assertEqual(self.d1.dict1['0'], test_val2)
        self.assertEqual(self.d2.dict1['0'], test_val2)
        self.assertEqual(self.d2.dict1[0], test_val1)


class TestSharedQueue(unittest.TestCase):
    def setUp(self) -> None:
        self.q1 = TestSimplePropertySyncClass()
        self.q2 = TestSimplePropertySyncClass(instance_id=self.q1._instance_id)

    def test_set_get(self):
        val1 = 'fuck'
        self.assertRaises(IndexError, lambda: self.q1.queue1.pop())
        self.q1.queue1.push(val1)
        self.assertEqual(self.q2.queue1.bpop(), val1)
        self.assertRaises(IndexError, lambda: self.q1.queue1.pop())
        self.q1.queue1.push(*list(range(10)))
        for i in range(10):
            self.assertEqual(self.q2.queue1.pop(), i)
        self.assertRaises(IndexError, lambda: self.q1.queue1.pop())
# TODO add a Blocked pop here



if __name__ == '__main__':
    unittest.main()
