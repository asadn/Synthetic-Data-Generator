""" This module constructs the model from given training data """
import pandas as pd
import sys
from datagenerator.models.types import *
from datagenerator.pyfiles.general import generate_hash_string

class ModelTrainer(object):
    """ encapsulates the data and methods required to extract pattern from data"""

    def __init__(self, file, header, dependencies, timestamp_cols = None,
                 timestamp_format=None, overrides=None):
        if header == "True":
            self.data = pd.read_csv(file, header=0)
        else:
            self.data = pd.read_csv(file, header=None)

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


    def get_header_dtypes(self,overrides):
        """ Extracts datatypes of each column from the data """
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
                sys.exit(0)

    def get_level(self):
        """ Get level of each node in Bayesian Network tree given dependencies """
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
                if parents_exists == True:
                    level[node] = max(parents_level) + 1
                    del nodes[i]
            i += 1
            iter_no += 1
            if iter_no == max_iter:
                print "Infinite loop in determining levels"
                return {}
        return level

    def get_varchar_cols(self):
        """ Get varchar columns from the data """
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
                                sys.exit(0)
        return varchar_cols

    def get_varchar_nodes(self,varchar_cols):
        """ Extract pattern and store it in objects of type VarcharCol """
        node_data = {}
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

        # Extract cpt from data
        for index,row in self.data:
            for col in varchar_cols:
                hash_string = generate_hash_string(row, self.dependencies[col])
                print hash_string




    def get_model(self):
        """ Extracts Tree model from the data """
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
        # timestamp columns and columns whose parent is timestamp
