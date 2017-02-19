""" This module redifine certain datatypes to be used  for data generation"""
import sys
import numpy
from numpy import random
from datetime import datetime
import datetime
from datetime import timedelta
from datagenerator.pyfiles.randomGen import data_genNorm
from datagenerator.pyfiles.general import generate_dates
from datagenerator.pyfiles.general import generate_hash_string
from collections import defaultdict


class Column(object):
    """ Parent class of all columns """

    def __init__(self, name, col_type, position, level, is_root="No",
                 parents=None, parentscount=None):
        self.name = name
        self.col_type = col_type
        self.is_root = is_root
        self.position = position
        self.level = level
        if parents is None:
            self.parents = []
            self.parentscount = parentscount
        else:
            self.parents = parents
            self.parentscount = parentscount

class VarcharCol(Column):
    """ Used to store float data type information """
    col_type = "varchar"

    def __init__(self, c_p_t, name, position, level, is_root, parents,
            parentscount):
        super(VarcharCol, self).__init__(name, self.col_type, position, level,
                is_root, parents, parentscount)
        self.c_p_t = c_p_t

    def generate_value(self,rec):
        """ Gets value from CPT for given parents """
        hash_string = generate_hash_string(rec,self.parents)
        col_value = data_genNorm(self.c_p_t[hash_string].keys(),
                                 numpy.array(self.c_p_t[hash_string].values()),1)
        return str(col_value)


class FloatCol(Column):
    """ Used to store float data type information """
    col_type = "float"

    def __init__(self, bandwidth, c_p_t, name, position, level, is_root, parents,
                 parentscount):
        super(FloatCol, self).__init__(name, self.col_type, position, level,
                                       is_root, parents, parentscount)
        self.bandwidth = bandwidth
        self.c_p_t = c_p_t

    def get_value(self, bin_val,bw):
        """ Returns a random value generated using the bin and bandwidth"""
        value = random.uniform(bin_val, bin_val+bw, 1)
        #value = 4.5
        return float(value)

    def generate_value(self,rec):
        """ Gets value from CPT for given parents """
        hash_string = generate_hash_string(rec,self.parents)
        tmp_col_value = data_genNorm(self.c_p_t[hash_string].keys(),
                                 numpy.array(self.c_p_t[hash_string].values()),1)
        col_value = self.get_value(float(tmp_col_value),self.bandwidth[hash_string])
        return str(col_value)

class IntCol(Column):
    """ Used to store int data type information """
    col_type = "int"

    def __init__(self, bandwidth, c_p_t, name, position, level, is_root, parents,
                 parentscount):
        super(IntCol, self).__init__(name, self.col_type, position, level,
                                     is_root, parents, parentscount)
        self.bandwidth = bandwidth
        self.c_p_t = c_p_t

    def get_value(self, bin_val,bw):
        """ Returns a random value generated using the bin and bandwidth"""
        value = random.uniform(bin_val, bin_val+bw, 1)
        return int(value)

    def generate_value(self,rec):
        """ Gets value from CPT for given parents """
        hash_string = generate_hash_string(rec,self.parents)
        tmp_col_value = data_genNorm(self.c_p_t[hash_string].keys(),
                                 numpy.array(self.c_p_t[hash_string].values()),1)
        col_value = self.get_value(int(tmp_col_value),self.bandwidth[hash_string])
        return str(col_value)

class TimestampCol(Column):
    """ Modified version of timestamp type """
    col_type = "timestamp"

    def __init__(self, ts_format, children, time_bucket, time_probs, number_eventsPH,
                 name, position, level, is_root, parents,
                 parentscount):
        super(TimestampCol, self).__init__(name, self.col_type, position, level,
                                           is_root, parents, parentscount)
        self.ts_format = ts_format
        self.children = children
        self.time_bucket = time_bucket
        self.time_probs = time_probs
        self.number_eventsPH = number_eventsPH

    def print_date(self, _timestamp):
        """ Returns the formatted timestamp """
        return _timestamp.strftime(self.ts_format)


class ProbabilityDist(object):
    """ Stores the probability distributions of a random variable """
    name = "probabilitydist"

    def __init__(self, probability_dict):
        try:
            if not isinstance(probability_dict, dict):
                raise TypeError
            elif abs(sum(probability_dict.values()) - 1) > 0.00001:
                raise ValueError
        except TypeError:
            print "invalid type in ProbabilityDist"
            self.probability_dict = None
            sys.exit(0)
        except ValueError:
            print "invalid value in ProbabilityDist"
            self.probability_dict = None
            sys.exit(0)
        else:
            self.probability_dict = probability_dict

class Tree(object):
    """ Manages an entire tree (collection of columns); Contains methods
    for printing data using tree"""

    def __init__(self, columns, header):
        self.columns = columns
        self.header = header

    def generate_csv(self,record,filename):
        out_file = open("tests/out_data/"+filename,"a")
        record_value = record[self.header[0]]
        for col_name in self.header[1:]:
            record_value += "," + record[col_name]
        out_file.write(record_value+"\n")
        out_file.close()

    def generate_ts_records(self, root, datetime_val, time_val):
        """ Generate records with children for a given date and hour """
        tmp_records = []
        records = []
        for child in root.time_probs.keys():
            for childhash in root.time_probs[child].keys():
                t_prob = root.time_probs[child][childhash].get(time_val, 0)
                # if t_prob > 0:
                #     print time_val
                if (data_genNorm(["active", "inactive"],
                    [t_prob, 1-t_prob], 1) == "active"):
                    no_of_events = random.poisson(
                            root.number_eventsPH[child][childhash].get(time_val, 0))
                    tmp_records.extend([childhash]*no_of_events)
        for rec in tmp_records:
            r_val = rec.split(";")
            _iter = 0
            new_record = {}
            for child in root.children:
                new_record[child] = r_val[_iter]
                _iter += 1
                new_record[root.name] = root.print_date(datetime_val)
            records.append(new_record)
        return records

    def generate_data(self, filename, counts=None, _start=None, _end=None):
        """ Generate data for given tree """

        out_file = open("tests/out_data/"+filename,"w")
        out_file.write(",".join(self.header) + "\n")
        out_file.close()
        records = []
        #root = self.columns[0][0]
        count_roots = 0
        root = 'Null'
        col_dict = defaultdict(list)
        for node in self.columns:
            col_dict[node.level].append(node)

        root = col_dict[0][0]
        if root == 'Null':
            print "Unable to retrieve root"
            sys.exit(0)
        if root.col_type == "timestamp":
            if (_start is None) or (_end is None):
                print "Error: Please specify start and end dates"
                sys.exit(0)

            dates = generate_dates(_start, _end)
            for _date in dates:
                for _hour in range(0, 24):
                    if root.time_bucket == "weekhour":
                        wday = _date.weekday()
                        time_val = 24*60*wday + _hour*60
                        datetime_val = _date + timedelta(hours=_hour)
                        if ((_date + timedelta(hours = _hour) >= _start) and
                            (_date + timedelta(hours = _hour) <= _end)):
                            records.extend(self.generate_ts_records(root, datetime_val, time_val))
                    # In case of week minute add an else condition here
        else:
            print "Non timestamp root"
            # TODO: Include the logic for non timestamp roots
            # records = contain values of roots

        col_keys = (col_dict).keys()
        col_keys.sort()
        for rec in records:
            for level in col_keys:
                if level > 1:
                    for col in col_dict[level]:
                        rec[col.name] = col.generate_value(rec)
            self.generate_csv(rec,filename)
        return records
