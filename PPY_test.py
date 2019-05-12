import unittest
from PPY import SharedClass, SharedProperty


class TestSimplePropertySyncClass(SharedClass):
    property1 = SharedProperty()

    def __init__(self, redis_pool=None, instance_id=None):
        super().__init__(redis_pool, instance_id)


class TestSimplePropertySync(unittest.TestCase):
    def setUp(self) -> None:
        super().__init__()
        self.c1 = TestSimplePropertySyncClass()
        self.c2 = TestSimplePropertySyncClass(instance_id=self.c1._instance_id)

    def test_setter_getter_self(self):
        val = '12341421'
        self.c1.property1 = val
        self.assertEqual(val, self.c1.property1, 'get from same property not consistence')
        self.assertEqual(val, self.c2.property1, 'get from same property not consistence')
        self.assertEqual(val, self.c1.property1, 'get from same property not consistence')
        self.assertEqual(val, self.c2.property1, 'get from same property not consistence')


if __name__ == '__main__':
    unittest.main()
