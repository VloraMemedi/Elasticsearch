
import unittest

from elastic_evaluation.elasticevaluation.core import minimum_el_sc2
# from elastic_evaluation.elasticevaluation import
from datetime import time
import time
from unittest import TestCase
TestCase.maxDiff = None

num_param1 = 'NEI00008'
num_param2 = 'NEI00009'
enum_param = 'NGY20011'
enum_param2 = "NAC22041"

start_date = 1451606400
day = 1451692800
week = 1452124800
month  = 1454198400
year = 1482192000
threemonths = 1459468800

localhost = 'localhost'
port = 9200
doc_type ="_doc"


class TestCalculations(unittest.TestCase):
    def test_min_day(self):
        time0 = time.time()
        minimum = minimum_el_sc2("aday-minimumtest5", localhost, port, num_param1, start_date, day, doc_type)
        time1 = time.time()
        dt = time1 - time0
        self.assertTrue(dt < 3)
        print("test min day in seconds:", dt)

    # def test_min_week(self):
    #     time0 = time.time()
    #     minimum = core.minimum_el_sc2("aweek-minimumtest5", localhost, port, num_param1, start_date, week, doc_type)
    #     time1 = time.time()
    #     dt = time1 - time0
    #     self.assertTrue(dt < 13)
    #     print('test min week run seconds: ', dt)
    #
    # def test_min_year(self):
    #     time0 = time.time()
    #     minimum = core.minimum_el_sc2("ayear-minimumtest5", localhost, port, num_param1, start_date, year, doc_type)
    #     time1 = time.time()
    #     dt = time1 - time0
    #     self.assertTrue(dt < 160)
    #     print("test min year in seconds:", dt)
    #
    # def test_min_day_sc3(self):
    #     time0 = time.time()
    #     minimum = core.minimum_el_sc3("ayear-minimumtest5", localhost, port)
    #     time1 = time.time()
    #     dt = time1 - time0
    #     self.assertTrue(dt < 160)
    #     print("SC3: test min day in seconds:", dt)


    # def test_max_day(self):
    #     time0 = time.time()
    #     maximum = elasticevaluation.maximum_el_sc2("aday-maximumtest5", localhost, port, num_param1, start_date, day, doc_type)
    #     time1 = time.time()
    #     dt = time1 - time0
    #     self.assertTrue(dt < 3)
    #     print("test min day in seconds:", dt)
    #
    # def test_max_week(self):
    #     time0 = time.time()
    #     maximum = elasticevaluation.maximum_el_sc2("aweek-maximumtest5", localhost, port, num_param1, start_date, week, doc_type)
    #     time1 = time.time()
    #     dt = time1 - time0
    #     self.assertTrue(dt < 13)
    #     print('test max week run seconds: ', dt)
    #
    # def test_max_year(self):
    #     time0 = time.time()
    #     maximum = elasticevaluation.maximum_el_sc2("ayear-maximumtest5", localhost, port, num_param1, start_date, year, doc_type)
    #     time1 = time.time()
    #     dt = time1 - time0
    #     self.assertTrue(dt < 160)
    #     print("test max year in seconds:", dt)
    #
    # def test_max_day_sc3(self):
    #     time0 = time.time()
    #     maximum = elasticevaluation.maximum_el_sc3("ayear-maximumtest5", localhost, port)
    #     time1 = time.time()
    #     dt = time1 - time0
    #     self.assertTrue(dt < 160)
    #     print("test max year in :", dt)
    # #

if __name__ == '__main__':
    unittest.main()