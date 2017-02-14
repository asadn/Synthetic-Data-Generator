""" Test different methods in training process """

from nose.tools import assert_equal
from nose.tools import assert_raises
from datagenerator.training.training_model import *
from datetime import datetime

""" Test Training """

class TestTraining(object):
    @classmethod
    def setupClass(self):
        filename = "tests/in_data/sub_webdata.csv"
        overrides = {"port":"varchar", "httpcode":"varchar"}
        dependencies = {"username":["timestamp"],
                        "timestamp":[],
                        "agent":["username"],
                        "source":["username"],
                        "sourceip":["source"],
                        "url1":["username"],
                        "url2":["url1"],
                        "dest_ip":["url1"],
                        "port":["username"],
                        "bytesin":["url1"],
                        "bytesout":["url1"],
                        "httpcode":["username","url1"],
                        "httpmethod":["username","url1"]}
        self.training_data = ModelTrainer(filename=filename,
                                    header="True",
                                    dependencies=dependencies,
                                    timestamp_cols=['timestamp'],
                                    timestamp_format=['%y-%m-%d %h:%M:%s'],
                                    overrides = overrides)
        self.training_data.print_time_taken()
        print self.training_data.model

    def test_get_header_dtypes(self):
        """ Test get_data() method """
        assert_equal(self.training_data.header,{"username":"varchar",
                                           "timestamp":"timestamp",
                                           "agent":"varchar",
                                           "source":"varchar",
                                           "sourceip":"varchar",
                                           "url1":"varchar",
                                           "url2":"varchar",
                                           "dest_ip":"varchar",
                                           "port":"varchar",
                                           "bytesin":"int",
                                           "bytesout":"int",
                                           "httpcode":"varchar",
                                           "httpmethod":"varchar"})



    def test_get_model(self):
        """ Test get_model function """
        assert_equal(True,False)

    def test_get_varchar_cols(self):
        """ Test get_varchar_cols function """
        returned_data = (self.training_data.get_varchar_cols())
        expected_data = ["username","agent","source","sourceip","url1","url2",
        "dest_ip","port","httpcode","httpmethod"]
        returned_data.sort()
        expected_data.sort()
        assert_equal(returned_data,expected_data)

    def test_get_numeric_cols(self):
        """ Test get_numeric_cols function """
        returned_data = (self.training_data.get_numeric_cols())
        expected_data = ["bytesin","bytesout"]
        returned_data.sort()
        expected_data.sort()
        assert_equal(returned_data,expected_data)

    def test_get_timestamp_cols(self):
        """ Test get_timestamp_cols function """
        returned_data = (self.training_data.get_timestamp_cols())
        expected_data = ["timestamp","username"]
        returned_data.sort()
        expected_data.sort()
        assert_equal(returned_data,expected_data)

    def test_get_levels(self):
        """ Test get_varchar_cols function """
        returned_levels = self.training_data.get_level()
        expected_levels = {"username":1, "timestamp":0, "agent":2, "source":2,
                           "sourceip":3, "url1":2, "url2":3, "dest_ip":3, "port":2,
                           "bytesin":3, "bytesout":3, "httpcode":3, "httpmethod":3}
        assert_equal(returned_levels,expected_levels)
