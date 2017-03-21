""" This module constructs the model from given training data """
import pandas as pd
import sys
import time
import numpy as np
from datagenerator.models.types import *
from datagenerator.pyfiles.general import generate_hash_string
from datagenerator.pyfiles.general import kernel_density_estimate
from datagenerator.pyfiles.general import bin_frequencies
from datagenerator.pyfiles.general import extract_weekminute_probs
import logging
import json


class ModelTrainer(object):
    """ encapsulates the data and methods required to extract pattern from data"""

    def __init__(self, filename, header, dependencies, timestamp_cols=[],
                 timestamp_format=None, overrides=None, logger=None):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Model trainer")
        self.logger.info("Extracting patterns from "+filename)
        self.time_taken = {}


        if header == "True":
            self.data = pd.DataFrame(pd.read_csv(filename, header=0))
        else:
            self.data = pd.read_csv(filename, header=None)
        # TODO: handle columns with NaN
        self.data.fillna(0)

        if timestamp_cols is None:
            self.root = "non-timestamp"
        else:
            self.root = "timestamp"
            self.timestamp_cols = []
            try:
                for col in timestamp_cols:
                    if col not in map(str, tuple(self.data.columns)):
                        self.logger.error(col + " is not present in header of given data")
                        raise ValueError
                    else:
                        self.timestamp_cols.append(col)
            except ValueError:
                self.logger.error("Timestamp column doesn't match columns in data header")
                sys.exit(0)

        self.timestamp_format = timestamp_format
        self.get_header_dtypes(overrides)
        self.dependencies = dependencies
        self.levels = self.get_level()
        self.model = self.get_model()
        self.write_model(filename)

    def write_model(self,filename):
        fw = open(filename+"_model.json","w")
        for col in self.model:
            self.logger.debug("writing json of "+col.name)
            fw.write(str(col.__dict__) + "\n")
        fw.close()

    def print_time_taken(self):
        for time_key in self.time_taken.keys():
            self.logger.info(time_key + ":" + str(self.time_taken[time_key]))
        self.logger.info("Total time :" + str(sum(self.time_taken.values())))

    def get_header_dtypes(self, overrides):
        """ Extracts datatypes of each column from the data """
        START_TIME = time.time()
        header_list = map(str, tuple(self.data.columns))
        self.header = {}
        for col in header_list:
            if (col not in self.timestamp_cols) and (col not in overrides.keys()):
                if isinstance((self.data[[col]].iloc[1])[0], int):
                    self.header[col] = "int"
                elif isinstance((self.data[[col]].iloc[1])[0], float):
                    self.header[col] = "float"
                else:
                    self.header[col] = "varchar"
            elif col in self.timestamp_cols:
                self.header[col] = "timestamp"
            elif col in overrides.keys():
                self.header[col] = overrides[col]
            else:
                self.logger.info("Unable to determine column type")
                self.time_taken["get_header_dtypes"] = (time.time() - START_TIME)
                sys.exit(0)
        self.logger.debug("Extracted types for columns : " +
                          ' '.join('{}:{}'.format(key,val) for key,val in self.header.items()))
        self.time_taken["get_header_dtypes"] = (time.time() - START_TIME)

    def get_level(self):
        """ Get level of each node in Bayesian Network tree given dependencies """
        START_TIME = time.time()
        level = {}
        i = 0
        iter_no = 0
        nodes = self.dependencies.keys()
        max_iter = len(nodes)*(len(nodes) + 1)/2
        while len(nodes) > 0:
            if i >= 0:
                i = i % len(nodes)
            node = nodes[i]
            parents = self.dependencies[node]
            if len(parents) == 0:
                level[node] = 0
                del nodes[i]
            else:
                parents_exists = True
                parents_level = []
                for p in parents:
                    if level.has_key(p):
                        parents_level.append(level[p])
                    else:
                        parents_exists = False
                        break
                if parents_exists is True:
                    level[node] = max(parents_level) + 1
                    del nodes[i]
            i += 1
            iter_no += 1
            if iter_no == max_iter:
                self.logger.info("Infinite loop in determining levels")
                self.time_taken["get_level"] = (time.time() - START_TIME)
                return {}
        self.logger.debug("Extracted levels for columns : " +
                          ' '.join('{}:{}'.format(key,val) for key,val in level.items()))
        self.time_taken["get_level"] = (time.time() - START_TIME)
        return level

    def get_varchar_cols(self):
        """ Get varchar columns from the data """
        START_TIME = time.time()
        varchar_cols = []
        for h_col in self.header.keys():
            if self.header[h_col] == 'varchar':
                if h_col not in varchar_cols:
                    varchar_cols.append(h_col)
                    for parent in self.dependencies[h_col]:
                        try:
                            if self.header[parent] in ["varchar", "timestamp"]:
                                if h_col not in varchar_cols:
                                    varchar_cols.append(h_col)
                            else:
                                raise ValueError
                        except ValueError:
                            self.logger.error(h_col + " has a numeric parent!!")
                            self.time_taken["get_varchar_cols"] = (time.time() - START_TIME)
                            sys.exit(0)
        self.time_taken["get_varchar_cols"] = (time.time() - START_TIME)
        return varchar_cols

    def get_numeric_cols(self):
        """ Get numeric columns from the data """
        START_TIME = time.time()
        numeric_cols = []
        for h_col in self.header.keys():
            if self.header[h_col] in ['int', 'float']:
                if h_col not in numeric_cols:
                    numeric_cols.append(h_col)
                    for parent in self.dependencies[h_col]:
                        try:
                            if self.header[parent] in ["varchar", "timestamp"]:
                                if h_col not in numeric_cols:
                                    numeric_cols.append(h_col)
                            else:
                                raise ValueError
                        except ValueError:
                            self.logger.info(h_col + " has a numeric parent!!")
                            self.time_taken["get_numeric_cols"] = (time.time() - START_TIME)
                            sys.exit(0)
        self.time_taken["get_numeric_cols"] = (time.time() - START_TIME)
        return numeric_cols

    def get_timestamp_cols(self):
        """ Get numeric columns from the data """
        START_TIME = time.time()
        timestamp_cols = []
        for h_col in self.header.keys():
            if self.header[h_col] in ['timestamp']:
                if h_col not in timestamp_cols:
                    timestamp_cols.append(h_col)
            for parent in self.dependencies[h_col]:
                if self.header[parent] in ["timestamp"]:
                    if h_col not in timestamp_cols:
                        timestamp_cols.append(h_col)
        self.time_taken["get_timestamp_cols"] = (time.time() - START_TIME)
        return timestamp_cols

    def get_varchar_node(self, varchar_cols):
        """ Extract pattern and store it in objects of type VarcharCol """
        START_TIME = time.time()
        node_data = {}
        sub_cols = []
        # define column nodes of type VarcharCol for each column
        for v_col in varchar_cols:
            new_node = VarcharCol(c_p_t={},
                                  name=v_col,
                                  position=(list(self.data.columns)).index(v_col),
                                  level=self.levels[v_col],
                                  is_root=("Yes" if (self.levels[v_col] == 0) else "No"),
                                  parents=self.dependencies[v_col],
                                  parentscount={})
            node_data[v_col] = new_node
            sub_cols.append(v_col)
            sub_cols.extend(new_node.parents)

        sub_cols = list(set(sub_cols))
        # Extract cpt from data
        for index, row in self.data[sub_cols].iterrows():
            for col in varchar_cols:
                if node_data[col].level != 0:
                    hash_string = generate_hash_string(row, self.dependencies[col])
                    if node_data[col].c_p_t.has_key(hash_string):
                        if node_data[col].c_p_t[hash_string].has_key(row[col]):
                            node_data[col].c_p_t[hash_string][row[col]] += 1
                        else:
                            node_data[col].c_p_t[hash_string][row[col]] = 1
                    else:
                        node_data[col].c_p_t[hash_string] = {}
                        node_data[col].c_p_t[hash_string][row[col]] = 1

                    # Update Parents count
                    if node_data[col].parentscount.has_key(hash_string):
                        node_data[col].parentscount[hash_string] += 1
                    else:
                        node_data[col].parentscount[hash_string] = 1
                else:
                    # In case of root node
                    if node_data[col].c_p_t.has_key(row[col]):
                        node_data[col].c_p_t[row[col]] += 1
                    else:
                        node_data[col].c_p_t[row[col]] = 1
                #TODO : get probability using parents count
        self.time_taken["get_varchar_nodes"] = (time.time() - START_TIME)
        return node_data

    def get_numeric_nodes(self, numeric_cols):
        """ Extract patterns and store it in type IntCol/ FloatCol """
        START_TIME = time.time()
        node_data = {}
        print numeric_cols
        # Define columns of type IntCol/ FloatCol
        sub_cols = []
        for col in numeric_cols:
            if self.header[col] == "int":
                new_node = IntCol(bandwidth={},
                                  c_p_t={},
                                  name=col,
                                  position=(list(self.data.columns)).index(col),
                                  level=self.levels[col],
                                  is_root=("Yes" if (self.levels[col] == 0) else "No"),
                                  parents=self.dependencies[col],
                                  parentscount={})
            elif self.header[col] == "float":
                new_node = FloatCol(bandwidth={},
                                    c_p_t={},
                                    name=col,
                                    position=(list(self.data.columns)).index(col),
                                    level=self.levels[col],
                                    is_root=("Yes" if (self.levels[col] == 0) else "No"),
                                    parents=self.dependencies[col],
                                    parentscount={})
            sub_cols.extend(self.dependencies[col])
            sub_cols.append(col)
            node_data[col] = new_node

        sub_cols = list(set(sub_cols))
        distinctparentsdata = {}

        """ Extract value list for each parent combination """

        for index, row in self.data[sub_cols].iterrows():
            for col in numeric_cols:
                if node_data[col].level !=0:
                    parents_hash = generate_hash_string(row, node_data[col].parents)
                    if distinctparentsdata.has_key(col):
                        if distinctparentsdata[col].has_key(parents_hash):
                            distinctparentsdata[col][parents_hash].append(row[col])
                        else:
                            distinctparentsdata[col][parents_hash] = [row[col]]
                    else:
                        distinctparentsdata[col] = {}
                        distinctparentsdata[col][parents_hash] = [row[col]]
                else:
                    if distinctparentsdata.has_key(col):
                        distinctparentsdata[col].append(row[col])
                    else:
                        distinctparentsdata[col] = [row[col]]

        """ Extract probability and bandwidth """

        for col in numeric_cols:
            if node_data[col].level !=0:
                for parentskey in distinctparentsdata[col].keys():
                    # Get Kernel density estimates for given data
                    # bandwidth, kde_vals = kernel_density_estimate(distinctparentsdata[col][parentskey])
                    bandwidth, kde_vals = bin_frequencies(distinctparentsdata[col][parentskey])

                    self.logger.debug("bandwidth for parents "+parentskey+" = "+str(bandwidth))
                    # Remove bins with 0 probability in kde_vals
                    # for bin_val in kde_vals.keys():
                    #     if kde_vals[bin_val] < (0.000000000001):
                    #         del kde_vals[bin_val]

                    node_data[col].c_p_t[parentskey] = kde_vals
                    node_data[col].bandwidth[parentskey] = bandwidth
            else:
                    bandwidth, kde_vals = bin_frequencies(distinctparentsdata[col])

                    self.logger.debug("bandwidth for"+col+" = "+str(bandwidth))
                    # Remove bins with 0 probability in kde_vals
                    # for bin_val in kde_vals.keys():
                    #     if kde_vals[bin_val] < (0.000000000001):
                    #         del kde_vals[bin_val]

                    node_data[col].c_p_t = kde_vals
                    node_data[col].bandwidth = bandwidth

        self.time_taken["get_numeric_nodes"] = (time.time() - START_TIME)
        return node_data

    def get_timestamp_nodes(self, timestamp_cols):
        """ Extract patterns and store it in type IntCol/ FloatCol """
        START_TIME = time.time()
        node_data = {}
        return_data = []
        # Define columns of type IntCol/ FloatCol
        for col in timestamp_cols:
            if self.header[col] == "timestamp":
                new_node = TimestampCol(ts_format=self.timestamp_format,
                                        children=[],
                                        time_bucket="weekhour", # TODO:need to parameterize it
                                        time_probs={},
                                        number_eventsPH={},
                                        name=col,
                                        position=(list(self.data.columns)).index(col),
                                        level=self.levels[col],
                                        is_root=("Yes" if (self.levels[col] == 0) else "No"),
                                        parents=None,
                                        parentscount=None)
                root_node = col
            elif self.header[col] == "varchar":
                new_node = VarcharCol(c_p_t={},
                                      name=col,
                                      position=(list(self.data.columns)).index(col),
                                      level=self.levels[col],
                                      is_root=("Yes" if (self.levels[col] == 0) else "No"),
                                      parents=self.dependencies[col],
                                      parentscount={})
            node_data[col] = new_node
        # Update the children

        for col in timestamp_cols:
            if col != root_node:
                (node_data[root_node].children).append(col)

        # return_data = node_data[root_node].children

        for child in node_data[root_node].children: # Parallelize
            sub_cols = [root_node,child]
            child_vals = self.data[child].unique()
            for child_value in child_vals:
                sub_data = (self.data[sub_cols])
                sub_data = sub_data[sub_data[child] == child_value]
                sub_data[root_node] = pd.to_datetime(sub_data[root_node])

                #Extrach Week minute
                sub_data["weekMinute"] = sub_data[root_node].apply(lambda ts: ts.weekday()*24*60 +
                ts.hour*60 + ts.minute)

                ## Extract Minute probabilities using KDE
                # bandwidth, kde_time = kernel_density_estimate(sub_data["weekMinute"].tolist(),list(np.arange(0,10040)))
                # for bin_val in kde_time.keys():
                #     if kde_time[bin_val] < (0.000000000001):
                #         del kde_time[bin_val]

                ## Extracting probabilities using Frequencies
                # Extract Weekday counts
                sub_data_dates = sub_data[root_node].apply(lambda dt: datetime.datetime(dt.year,dt.month,dt.day,0,0))
                sub_data_dates = pd.DataFrame(sub_data_dates.unique())
                sub_data_dates[0] = pd.to_datetime(sub_data_dates[0])
                sub_data_weekday = sub_data_dates[0].apply(lambda ts: ts.weekday())
                weekday_counts = sub_data_weekday.value_counts().to_dict()

                if node_data[root_node].time_bucket == "weekhour":
                    # Week hour counts
                    sub_data_hour = sub_data[root_node].apply(lambda dt: datetime.datetime(dt.year,dt.month,dt.day,dt.hour))
                    sub_data_hour = pd.DataFrame(sub_data_hour.unique())
                    sub_data_hour[0] = pd.to_datetime(sub_data_hour[0])
                    sub_data_weekhour = sub_data_hour[0].apply(lambda ts: ts.weekday()*24*60 + ts.hour*60)
                    weekhour_counts = sub_data_weekhour.value_counts().to_dict()
                    weekday_counts = {0:10,1:10,2:10,3:10,4:10,5:10,6:10}
                    kde_hour = extract_weekminute_probs(weekhour_counts, weekday_counts)

                    #Extract events per time bucket
                    sub_data["DateHour"] = sub_data[root_node].apply(lambda dt: datetime.datetime(dt.year,dt.month,dt.day,dt.hour,0))
                    sub_data["WeekHour"] = sub_data[root_node].apply(lambda ts: ts.weekday()*24*60 +
                                                                                ts.hour*60)
                    grouped = sub_data.groupby(['DateHour','WeekHour'],as_index=False)
                    eventsPH = (((grouped.size().to_frame()).groupby(level=1)).mean())[0].to_dict()


                if node_data[root_node].time_probs.has_key(child):
                    node_data[root_node].time_probs[child][child_value] = kde_hour
                else:
                    node_data[root_node].time_probs[child] = {}
                    node_data[root_node].time_probs[child][child_value] = kde_hour

                if node_data[root_node].number_eventsPH.has_key(child):
                    node_data[root_node].number_eventsPH[child][child_value] = eventsPH
                else:
                    node_data[root_node].number_eventsPH[child] = {}
                    node_data[root_node].number_eventsPH[child][child_value] = eventsPH


        self.time_taken["get_timestamp_nodes"] = (time.time() - START_TIME)
        return node_data


    def get_model(self):
        """ Extracts Tree model from the data """
        START_TIME = time.time()
        # varchar columns whose parent is not timestamp
        varchar_cols = self.get_varchar_cols()
        tree_nodes = []
        # remove varchar columns that has timestamp as parents

        for v_col in varchar_cols:
            for parent in self.dependencies[v_col]:
                if self.header[parent] == "timestamp":
                    varchar_cols.remove(v_col)
                    break

        self.logger.info("Extracting varchar columns - " + ",".join(varchar_cols))
        varchar_node = self.get_varchar_node(varchar_cols)
        tree_nodes.extend(varchar_node.values())
        # int/float columns
        numeric_cols = self.get_numeric_cols()
        if len(numeric_cols) > 0:
            self.logger.info("Extracting numeric columns - " + ",".join(numeric_cols))
            numeric_nodes = self.get_numeric_nodes(numeric_cols)
        else:
            numeric_nodes = {}
            self.time_taken["get_numeric_nodes"] = 0
        tree_nodes.extend(numeric_nodes.values())
        self.time_taken["get_model"] = (time.time() - START_TIME -
                                        self.time_taken["get_varchar_cols"] -
                                        self.time_taken["get_varchar_nodes"] -
                                        self.time_taken["get_numeric_cols"] -
                                        self.time_taken["get_numeric_nodes"])
        # timestamp columns and columns whose parent is timestamp
        timestamp_cols = self.get_timestamp_cols()
        if len(timestamp_cols) > 0:
            self.logger.info("Extracting timestamp columns - " + ",".join(timestamp_cols))
            timestamp_nodes = self.get_timestamp_nodes(timestamp_cols)
        else:
            timestamp_nodes = {}
            self.time_taken["get_timestamp_nodes"] = 0
        tree_nodes.extend(timestamp_nodes.values())
        return tree_nodes
