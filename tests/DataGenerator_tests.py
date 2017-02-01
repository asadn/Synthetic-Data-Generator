""" Contains different tests """
from nose.tools import assert_equal
from nose.tools import assert_raises
from datagenerator.models.types import *
from datetime import datetime
from datagenerator.pyfiles.general import generate_dates

""" Test models """

class TestModels:

    def test_float(self):
        """ Tests class FloatType """
        val = FloatCol(bandwidth=100,
                       c_p_t=[],
                       name="BytesIn",
                       position=0,
                       level=0,
                       is_root="No",
                       parents=None,
                       parentscount=None)
        random_val = val.get_value(0)
        assert_equal((random_val < 100) and (random_val > 0), True)      # The value should be within range
        assert_equal(isinstance(random_val, float), True)     # The value should be of type float

    def test_int(self):
        """ Tests class IntType """
        val = IntCol(bandwidth=100,
                     c_p_t=[],
                     name="BytesOut",
                     position=0,
                     level=0,
                     is_root="No",
                     parents=None,
                     parentscount=None)
        random_val = val.get_value(0)
        assert_equal((random_val < 100) and (random_val > 0), True)      # The value should be within range
        assert_equal(isinstance(random_val, int), True)                     # The value should be of type int

    def test_timstamp(self):
        """ Tests class timestamp """
        dt = datetime.strptime("21/11/06 16:30", "%d/%m/%y %H:%M")
        type = TimestampCol(ts_format="%d/%m/%y %H:%M:%S",
                            children={},
                            wday_probs={},
                            hour_probs={},
                            number_eventsPH={},
                            name="EventTime",
                            position=0,
                            level=0,
                            is_root="No",
                            parents=None,
                            parentscount=None)
        assert_equal(type.print_date(dt), "21/11/06 16:30:00")



    def test_probabilitydist(self):
        """ Tests probability distributions type """
        prob_dist = ProbabilityDist({"a":0.5, "b":0.5})
        assert_equal(prob_dist.probability_dict, {"a":0.5, "b":0.5})
        assert_raises(SystemExit, ProbabilityDist, 1)
        assert_raises(SystemExit, ProbabilityDist, {"a":0.5, "b":0.6})

    def test_trees(self):
        """ Tests the tree types """
        cols = {}
        cols[0] = [TimestampCol(ts_format="%d/%m/%y %H:%M:%S",
                                children=["UserName"],
                                wday_probs={"asad":{1:0.9, 2:0.9, 3:0.9, 4:0.9, 5:0.8},
                                            "hari":{1:0.9, 2:0.9, 3:0.9, 4:0.9, 5:0.9}},
                                hour_probs={"asad":{8:0.9, 9:0.9, 10:0.9, 11:0.9, 12:0.9},
                                            "hari":{1:0.9, 2:0.9, 3:0.9, 4:0.9, 5:0.9}},
                                number_eventsPH={"asad":{8:10, 9:10, 10:10, 11:50, 12:50},
                                                 "hari":{1:10, 2:10, 3:10, 4:50, 5:50}},
                                name="EventTime",
                                position=0,
                                level=0,
                                is_root="Yes",
                                parents=None,
                                parentscount=None)]
        cols[1] = [VarcharCol(c_p_t={},
                              name="UserName",
                              position=1,
                              level=1,
                              is_root="No",
                              parents=["EventTime"],
                              parentscount=None)]
        cols[2] = [VarcharCol(c_p_t={"asad":{"Dest1":0.2, "Dest2":0.3, "Dest3":0.5},
                                     "hari":{"Dest2":0.2, "Dest3":0.3, "Dest4":0.5}},
                              name="Destination",
                              position=2,
                              level=2,
                              is_root="No",
                              parents=["UserName"],
                              parentscount=None)]
        header = ["EventTime","UserName","Destination"]
        tree_data = Tree(cols, header)
        records = tree_data.generate_data(_start=datetime.strptime("21/11/06 08:00", "%d/%m/%y %H:%M"),
                                _end=datetime.strptime("21/12/06 00:00", "%d/%m/%y %H:%M"),filename="data.csv")
        assert_equal(len(records)>1, True)


    def test_general_funcs(self):
        """ Test functions from general module """

        """ Test date generator function from general.py """

        _start = datetime.strptime("01/02/17 16:30", "%d/%m/%y %H:%M")
        _end = datetime.strptime("04/02/17 16:30", "%d/%m/%y %H:%M")
        assert_equal(generate_dates(_start,_end),
                     [datetime.strptime("01/02/17 00:00", "%d/%m/%y %H:%M"),
                      datetime.strptime("02/02/17 00:00", "%d/%m/%y %H:%M"),
                      datetime.strptime("03/02/17 00:00", "%d/%m/%y %H:%M"),
                      datetime.strptime("04/02/17 00:00", "%d/%m/%y %H:%M")])
