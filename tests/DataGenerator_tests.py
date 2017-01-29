""" Contains different tests """
from nose.tools import assert_equal
from nose.tools import assert_raises
from datagenerator.models.types import *
from datetime import datetime


""" Test models """

class TestModels:

    def test_float(self):
        """ Tests class FloatType """
        val = FloatCol(bandwidth=100,
                c_p_t=[],
                name="BytesIn",
                position = 0,
                is_root="No",
                parents=None,
                parentscount=None)
        random_val = val.get_value(0)
        assert_equal((random_val < 100) and (random_val > 0), True)      # The value should be within range
        assert_equal(isinstance(random_val, float), True)                   # The value should be of type float

    def test_int(self):
        """ Tests class IntType """
        val = IntCol(bandwidth=100,
                c_p_t=[],
                name="BytesOut",
                position = 0,
                is_root="No",
                parents=None,
                parentscount=None)
        random_val = val.get_value(0)
        assert_equal((random_val < 100) and (random_val > 0), True)      # The value should be within range
        assert_equal(isinstance(random_val, int), True)                     # The value should be of type int

    def test_timstamp(self):
        """ Tests class timestamp """
        dt = datetime.strptime("21/11/06 16:30", "%d/%m/%y %H:%M")
        type = TimestampCol(ts_format = "%d/%m/%y %H:%M:%S",
                c_p_t=[],
                name="EventTime",
                position = 0,
                is_root="No",
                parents=None,
                parentscount=None)
        assert_equal(type.print_date(dt),"21/11/06 16:30:00")

    def test_probabilitydist(self):
        """ Tests probability distributions type """
        prob_dist = ProbabilityDist({"a":0.5,"b":0.5})
        assert_equal(prob_dist.probability_dict, {"a":0.5,"b":0.5})
        assert_raises(TypeError, ProbabilityDist,(1))
        assert_raises(ValueError, ProbabilityDist,({"a":0.5,"b":0.6}))
