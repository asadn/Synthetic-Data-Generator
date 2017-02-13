""" This module constructs the model from given training data """
import pandas as pd
import sys
import time
from datagenerator.models.types import *
from datagenerator.pyfiles.general import generate_hash_string
from datagenerator.pyfiles.general import kernel_density_estimate

class ModelTrainer(object):
    """ encapsulates the data and methods required to extract pattern from data"""

    def __init__(self, filename, header, dependencies, timestamp_cols = None,
                 timestamp_format=None, overrides=None):
        self.time_taken = {}
        if header == "True":
            self.data = pd.DataFrame(pd.read_csv(filename, header=0))
        else:
            self.data = pd.read_csv(filename, header=None)

        self.timestamp_cols = []
        try:
            for col in timestamp_cols:
                if col not in map(str,tuple(self.data.columns)):
                    raise ValueError
                else:
                    self.timestamp_cols.append(col)
        except ValueError:
                print "Timestamp column doesn't match columns in data header"
                sys.exit(0)

        self.timestamp_format = timestamp_format
        self.get_header_dtypes(overrides)
        self.dependencies = dependencies
        self.levels = self.get_level()
        self.model = self.get_model()

    def print_time_taken(self):
        for time_key in self.time_taken.keys():
            print time_key,":",self.time_taken[time_key]
        print "Total time :",sum(self.time_taken.values())

    def get_header_dtypes(self,overrides):
        """ Extracts datatypes of each column from the data """
        START_TIME = time.time()
        header_list = map(str, tuple(self.data.columns))
        self.header = {}
        for col in header_list:
            if (col not in self.timestamp_cols) and (col not in overrides.keys()):
                if isinstance((self.data[[col]].iloc[1])[0],int):
                    self.header[col] = "int"
                elif isinstance((self.data[[col]].iloc[1])[0],float):
                    self.header[col] = "float"
                else:
                    self.header[col] = "varchar"
            elif col in self.timestamp_cols:
                self.header[col] = "timestamp"
            elif col in overrides.keys():
                self.header[col] = overrides[col]
            else:
                print "Unable to determine column type"
                self.time_taken["get_header_dtypes"] = (time.time() - START_TIME)
                sys.exit(0)
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
            if i>=0:
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
                print "Infinite loop in determining levels"
                self.time_taken["get_level"] = (time.time() - START_TIME)
                return {}
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
                                print h_col + " has a numeric parent!!"
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
                                print h_col + " has a numeric parent!!"
                                self.time_taken["get_numeric_cols"] = (time.time() - START_TIME)
                                sys.exit(0)
        self.time_taken["get_numeric_cols"] = (time.time() - START_TIME)
        return numeric_cols

    def get_varchar_nodes(self, varchar_cols):
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

        # Define columns of type IntCol/ FloatCol
        for col in numeric_cols:
            if self.header[col] == "int":
                new_node = IntCol(bandwidth={},
                                  c_p_t={},
                                  name=col,
                                  position=(list(self.data.columns)).index(col),
                                  level=self.levels[col],
                                  is_root="No",
                                  parents=self.dependencies[col],
                                  parentscount={})
            elif self.header[col] == "float":
                new_node = FloatCol(bandwidth={},
                                  c_p_t={},
                                  name=col,
                                  position=(list(self.data.columns)).index(col),
                                  level=self.levels[col],
                                  is_root="No",
                                  parents=self.dependencies[col],
                                  parentscount={})

            sub_cols = [col]
            sub_cols.extend(new_node.parents)
            distinct_parents = self.data[new_node.parents].drop_duplicates()
            for index, row in distinct_parents.iterrows():
                row = pd.DataFrame([tuple(row.values)], columns=row.index)
                sub_data = pd.merge(self.data[sub_cols], row, on=new_node.parents, how="inner")
                bandwidth, kde_vals = kernel_density_estimate(sub_data[col].tolist())

                # Remove bins with 0 probability in kde_vals
                for bin_val in kde_vals.keys():
                    if kde_vals[bin_val] < (0.000000000001):
                        del kde_vals[bin_val]


                # as values for key hash(parents)
                parents_hash = generate_hash_string(row,new_node.parents)
                # print row
                # print new_node.parents
                # print parents_hash
                if len(kde_vals) == 0:
                    print "Empty kde ",sub_data[col].tolist()
                else:
                    print kde_vals
                new_node.c_p_t[parents_hash] = kde_vals
                new_node.bandwidth[parents_hash] = bandwidth
            node_data[col] = new_node
        self.time_taken["get_numeric_nodes"] = (time.time() - START_TIME)
        return node_data


    def get_model(self):
        """ Extracts Tree model from the data """
        START_TIME = time.time()
        # varchar columns whose parent is not timestamp
        varchar_cols = self.get_varchar_cols()

        # remove varchar columns that has timestamp as parents
        for v_col in varchar_cols:
            for parent in self.dependencies[v_col]:
                if self.header[parent] == "timestamp":
                    varchar_cols.remove(v_col)
                    break

        varchar_nodes = self.get_varchar_nodes(varchar_cols)

        # int/float columns
        numeric_cols = self.get_numeric_cols()
        numeric_nodes = self.get_numeric_nodes(numeric_cols)
        self.time_taken["get_model"] = (time.time() - START_TIME -
                                        self.time_taken["get_varchar_cols"] -
                                        self.time_taken["get_varchar_nodes"] -
                                        self.time_taken["get_numeric_cols"] -
                                        self.time_taken["get_numeric_nodes"])
        return varchar_nodes
        # timestamp columns and columns whose parent is timestamp
