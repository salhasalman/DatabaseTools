# coding: utf-8
# pew in databasetools-venv python ./test/mongo.py

import os
import sys
sys.path.append('../')

import unittest
import doctest
import time
from databasetools import mongo
from databasetools.mongo import *

min = 0
max = 2
assert min <= max

print("==============\nStarting unit tests...")

if min <= 0 <= max:
    class DocTest(unittest.TestCase):
        def testDoctests(self):
            """Run doctests"""
            doctest.testmod(mongo)

if min <= 1 <= max:
    class Test1(unittest.TestCase):
        def test1(self):
            c = MongoCollection("mongo-test", "test1", host="localhost",
                                indexOn="user_id")
            c.resetCollection(security=False)
            self.assertTrue(len(c) == 0)
            c["a"] = {"value": "toto"} # __setitem__
            self.assertTrue(len(c) == 1)
            result = c.findOne({"user_id": "a"}, projection={"user_id": 1, "value": 1, "_id": 0})
            self.assertTrue(result == {"user_id": "a", "value": "toto"})
            del c["a"] # __delitem__
            self.assertTrue(len(c) == 0)
            c["b"] = {"value": "toto"}
            c["b"] = {"value": "toto2", "test": 1}
            self.assertTrue(len(c) == 1)
            result = c.findOne({"user_id": "b"}, projection={"timestamp": 0, "_id": 0})
            self.assertTrue(result == {"user_id": "b", "value": "toto2", "test": 1})
            result = c["b"] # __getitem__
            del result["timestamp"]
            del result["_id"]
            self.assertTrue(result == {"user_id": "b", "value": "toto2", "test": 1})
            self.assertTrue("b" in c) # __contains__
            self.assertTrue("a" not in c)
            self.assertTrue("c" not in c)
            c.resetCollection(security=False)

if __name__ == '__main__':
    unittest.main() # Or execute as Python unit-test in eclipse


    print("Unit tests done.\n==============")







