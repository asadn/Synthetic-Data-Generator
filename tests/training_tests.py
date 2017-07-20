""" Test different methods in training process """

from nose.tools import assert_equal
from nose.tools import assert_raises
from datagenerator.training.training_model import *
from datetime import datetime
import datetime
from datagenerator.models.types import *
import logging

logger = logging.getLogger(__name__)
logger.debug("Initialize")
format_log = "%(asctime)s - %(name)s - [%(levelname)s] - %(message)s"
logging.basicConfig(format = format_log, level=logging.DEBUG, filename="tests/logs/test.log")

""" Test Training functionality of data generator """

class TestTraining(object):
    @classmethod
    def setupClass(self):
        logger.debug("Setting up")
        filename = "tests/in_data/sample_webproxy_new.csv"
        overrides = {"port":"varchar", "httpResponse":"varchar"}
        dependencies = {"username":["timestamp"],
                        "timestamp":[],
                        "agent":["username"],
                        "sourceip":["username"],
                        "destination":["username"],
                        "destinationpIP":["destination"],
                        "bytes_in":["destination"],
                        "bytes_out":["destination"],
                        "httpResponse":["username","destination"],
                        "method":["username","destination"]}
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
                                           "sourceip":"varchar",
                                           "destination":"varchar",
                                           "destinationpIP":"varchar",
                                           "bytes_in":"int",
                                           "bytes_out":"int",
                                           "httpResponse":"varchar",
                                           "method":"varchar"})



    def test_get_model(self):
        """ Test get_model function """
        logger.debug("Test get_model function")
        logger.debug("Finished extracting Proxy data.... Generating")
        tree_data_proxy = Tree(self.training_data.model, self.training_data.header.keys())
        records = tree_data_proxy.generate_data(_start=datetime.datetime.strptime("2016-04-01 00:00", "%Y-%m-%d %H:%M"),
                                _end=datetime.datetime.strptime("2016-04-02 00:00", "%Y-%m-%d %H:%M"),filename="dummy.csv")
        logger.debug("Finished generating data....")
        assert_equal(len(records)>1, True)

    def test_get_varchar_cols(self):
        """ Test get_varchar_cols function """
        returned_data = (self.training_data.get_varchar_cols())
        expected_data = ["username","agent","sourceip",
        "destination","destinationpIP","httpResponse","method"]
        returned_data.sort()
        expected_data.sort()
        assert_equal(returned_data,expected_data)

    def test_get_numeric_cols(self):
        """ Test get_numeric_cols function """
        returned_data = (self.training_data.get_numeric_cols())
        expected_data = ["bytes_in","bytes_out"]
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
        expected_levels = {"username":1, "timestamp":0, "agent":2,
                           "sourceip":2, "destination":2, "destinationpIP":3,
                           "bytes_in":3, "bytes_out":3, "httpResponse":3, "method":3}
        assert_equal(returned_levels,expected_levels)

    def test_repo_data(self):
        logger.debug("\n\nTest get data with repo dataset\n=================\n")
        filename = "tests/in_data/RepoData.csv"
        overrides = {}
        repo_dependencies = {"user":["datetime"],
                        "datetime":[],
                        "ip_address":["user"],
                        "action":["destination"],
                        "destination":[]}
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
        logger.debug("...Finished Generating Repo data")
        assert_equal(len(repo_records)>1, True)


    def test_non_ts_root_data(self):
        logger.debug("Test get data with non-timestamp root function")
        filename = "tests/in_data/web_no_timestamp.csv"
        # filename = "tests/in_data/webdata.csv"
        overrides = {"port":"varchar", "httpResponse":"varchar"}
        dependencies = {"username":[],
                "agent":["username"],
                "source":["username"],
                "sourceip":["source"],
                "url1":["username"],
                "url2":["url1"],
                "destinationpIP":["url1"],
                "port":["username"],
                "bytes_in":["url1"],
                "bytes_out":["url1"],
                "httpResponse":["username","url1"],
                "httpmethod":["username","url1"]}
        training_data = ModelTrainer(filename=filename,
                                            header="True",
                                            dependencies=dependencies,
                                            overrides = overrides)
        training_data.print_time_taken()

        tree_data_nots = Tree(training_data.model, training_data.header.keys())
        rnots_records = tree_data_nots.generate_data(counts=10000,filename="nots_webdata.csv")
        assert_equal(len(rnots_records)>1, True)
    #
    def test_iris_data(self):
        logger.debug("\n\nTest get data with iris dataset\n=================\n")
        filename = "tests/in_data/irisdata.csv"
        # filename = "tests/in_data/webdata.csv"
        overrides = {}
        dependencies = {"class":[],
                "sepal_length":["class"],
                "sepal_width":["class"],
                "petal_length":["class"],
                "petal_width":["class"]}
        training_data = ModelTrainer(filename=filename,
                                            header="True",
                                            dependencies=dependencies,
                                            overrides = overrides)
        training_data.print_time_taken()

        tree_data_nots = Tree(training_data.model, training_data.header.keys())
        rnots_records = tree_data_nots.generate_data(counts=500,filename="irisdata_syn.csv")
        assert_equal(len(rnots_records)>1, True)

    def test_ad_data(self):
        logger.debug("\n\nTest get data with AD dataset\n=================\n")
        filename = "tests/in_data/AD_data.csv"
        overrides = {"externalId":"varchar","pages":"varchar","bytes":"varchar"}
        dependencies = {"userId":["deviceReceiptTime"],
                        "categoryOutcome":["userId","externalId","destination"],
                        "externalId":["userId","destination"],
                        "destination":[],
                        "objectType":["objName"],
                        "objName":["userId","externalId"],
                        "calling_st_ID":["userId","externalId"],
                        "country":["userId","externalId"],
                        "deviceReceiptTime":[]}
        print("Extracting AD data....")
        repo_training_data = ModelTrainer(filename=filename,
                                    header="True",
                                    dependencies=dependencies,
                                    timestamp_cols=['deviceReceiptTime'],
                                    timestamp_format=['%Y-%m-%dT%H:%M:%S'],
                                    overrides = overrides)
        repo_training_data.print_time_taken()
        logger.debug("Finished extracting AD data.... Generating")
        ad_tree_data = Tree(repo_training_data.model, repo_training_data.header.keys())
        records = ad_tree_data.generate_data(_start=datetime.datetime.strptime("21/11/06 08:00", "%d/%m/%y %H:%M"),
                                _end=datetime.datetime.strptime("21/12/06 00:00", "%d/%m/%y %H:%M"),filename="ADdata.csv")
        logger.debug("Finished Generating AD data")
        assert_equal(len(records)>1, True)

    def test_real_proxy_data(self):
        logger.debug("\n\nTest real webproxy dataset\n=================\n")
        filename = "tests/in_data/all_real_proxy_subdata.csv"
        overrides = {"port":"varchar", "httpResponseStatus":"varchar"}
        dependencies = {"clientIp":["timestamp"],
                        "timestamp":[],
                        "timeSpent":["clientIp"],
                        "destHostName":["clientIp"],
                        "payloadSizeResponse":["clientIp","destHostName"],
                        "httpMethod":["destHostName","clientIp"],
                        "httpResponseStatus":["destHostName","clientIp"]}
        training_data = ModelTrainer(filename=filename,
                                            header="True",
                                            timestamp_cols=['timestamp'],
                                            timestamp_format='%Y-%m-%dT%H:%M:%S',
                                            dependencies=dependencies,
                                            overrides = overrides,delimitter=",")
        training_data.print_time_taken()

        tree_data_nots = Tree(training_data.model, training_data.header.keys())
        rnots_records = tree_data_nots.generate_data(_start=datetime.datetime.strptime("2005-04-28 00:00", "%Y-%m-%d %H:%M"),
                                _end=datetime.datetime.strptime("2005-05-30 00:00", "%Y-%m-%d %H:%M"),filename="realdata_syn_users.csv")
        assert_equal(len(rnots_records)>1, True)


    def test_ssc_proxy_data(self):
        logger.debug("\n\nTest real realAD dataset\n=================\n")
        filename = "tests/in_data/SSC.csv"
        overrides = {"port":"varchar", "httpResponseStatus":"varchar"}
        dependencies = {"clientIp":["timestamp"],
                        "timestamp":[],
                        "timeSpent":["clientIp","destHostName"],
                        "destHostName":["clientIp"],
                        "payloadSizeResponse":["destHostName"],
                        "httpMethod":["destHostName","clientIp"],
                        "httpResponseStatus":["destHostName","clientIp"]}
        training_data = ModelTrainer(filename=filename,
                                            header="True",
                                            timestamp_cols=['timestamp'],
                                            timestamp_format='%Y-%m-%dT%H:%M:%S',
                                            dependencies=dependencies,
                                            overrides = overrides,delimitter="\t")
        training_data.print_time_taken()

        tree_data_nots = Tree(training_data.model, training_data.header.keys())
        rnots_records = tree_data_nots.generate_data(_start=datetime.datetime.strptime("2005-04-05 00:00", "%Y-%m-%d %H:%M"),
                                _end=datetime.datetime.strptime("2005-04-30 00:00", "%Y-%m-%d %H:%M"),filename="realdata_ad.csv")
        assert_equal(len(rnots_records)>1, True)
