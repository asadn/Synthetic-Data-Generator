""" Test different methods in training process """

from nose.tools import assert_equal
from nose.tools import assert_raises
from datagenerator.training.training_model import *
from datetime import datetime
import datetime
from datagenerator.models.types import *

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
                                    timestamp_format='%Y-%m-%d %H:%M:%S',
                                    overrides = overrides)
        self.training_data.print_time_taken()

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
        print("Finished extracting Proxy data.... Generating")
        tree_data_proxy = Tree(self.training_data.model, self.training_data.header.keys())
        records = tree_data_proxy.generate_data(_start=datetime.datetime.strptime("21/11/06 08:00", "%d/%m/%y %H:%M"),
                                _end=datetime.datetime.strptime("21/12/06 00:00", "%d/%m/%y %H:%M"),filename="data.csv")
        print("Finished generating data....")
        assert_equal(len(records)>1, True)

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

    # def test_repo_data(self):
        filename = "tests/in_data/RepoData.csv"
        overrides = {}
        repo_dependencies = {"user":["datetime"],
                        "datetime":[],
                        "ip_address":["user"],
                        "action":["user","destination"],
                        "destination":["user"]}
        repo_training_data = ModelTrainer(filename=filename,
                                    header="True",
                                    dependencies=repo_dependencies,
                                    timestamp_cols=['datetime'],
                                    timestamp_format='%Y-%m-%d %H:%M:%S',
                                    overrides = overrides)
        repo_training_data.print_time_taken()

        tree_data_repo = Tree(repo_training_data.model, repo_training_data.header.keys())
        repo_records = tree_data_repo.generate_data(_start=datetime.datetime.strptime("2016-04-01 00:00", "%Y-%m-%d %H:%M"),
                                _end=datetime.datetime.strptime("2016-05-30 00:00", "%Y-%m-%d %H:%M"),filename="repodata.csv")
        assert_equal(len(repo_records)>1, True)

    # def test_ad_data(self):
    #     filename = "tests/in_data/AD_data2.csv"
    #     overrides = {"externalId":"varchar","pages":"varchar","bytes":"varchar"}
    #     dependencies = {"userId":["deviceReceiptTime"],
    #                     "categoryOutcome":["userId","externalId","destination"],
    #                     "externalId":["userId","destination"],
    #                     "destination":["userId"],
    #                     "objectType":["objName"],
    #                     "objName":["userId","externalId"],
    #                     "calling_st_ID":["userId","externalId"],
    #                     "pages":["userId"],
    #                     "bytes":["userId"],
    #                     "country":["userId","externalId"],
    #                     "deviceReceiptTime":[],
    #                     "name":["externalId"],
    #                     "deviceEventClassId":["externalId"]}
    #     print("Extracting AD data....")
    #     repo_training_data = ModelTrainer(filename=filename,
    #                                 header="True",
    #                                 dependencies=dependencies,
    #                                 timestamp_cols=['deviceReceiptTime'],
    #                                 timestamp_format=['%Y-%m-%d %H:%M:%S'],
    #                                 overrides = overrides)
    #     repo_training_data.print_time_taken()
    #     print("Finished extracting AD data.... Generating")
    #     ad_tree_data = Tree(repo_training_data.model, repo_training_data.header.keys())
    #     records = ad_tree_data.generate_data(_start=datetime.datetime.strptime("21/11/06 08:00", "%d/%m/%y %H:%M"),
    #                             _end=datetime.datetime.strptime("21/12/06 00:00", "%d/%m/%y %H:%M"),filename="ADdata.csv")
    #     assert_equal(len(records)>1, True)
