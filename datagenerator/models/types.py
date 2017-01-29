""" This module redifine certain datatypes to be used  for data generation"""
from numpy import random
from datetime import datetime

class Column(object):
    """ Parent class of all columns """

    def __init__(self, name, col_type, position, is_root="No",
                 parents=None, parentscount=None):
        self.name = name
        self.col_type = col_type
        self.is_root = is_root
        self.position = position
        if parents is None:
            self.parents = []
            self.parentscount = parentscount
        else:
            self.parents = parents
            self.parentscount = parentscount


class FloatCol(Column):
    """ Used to store float data type information """
    col_type = "float"

    def __init__(self, bandwidth, c_p_t, name, position, is_root, parents,
                 parentscount):
        super(FloatCol, self).__init__(name, self.col_type, position,
                                       is_root, parents, parentscount)
        self.bandwidth = bandwidth
        self.c_p_t = c_p_t

    def get_value(self, bin_val):
        """ Returns a random value generated using the bin and bandwidth"""
        value = random.uniform(bin_val, bin_val+self.bandwidth, 1)
        return float(value)

class IntCol(Column):
    """ Used to store int data type information """
    col_type = "float"

    def __init__(self, bandwidth, c_p_t, name, position, is_root, parents,
                 parentscount):
        super(IntCol, self).__init__(name, self.col_type, position,
                                     is_root, parents, parentscount)
        self.bandwidth = bandwidth
        self.c_p_t = c_p_t

    def get_value(self, bin_val):
        """ Returns a random value generated using the bin and bandwidth"""
        value = random.uniform(bin_val, bin_val+self.bandwidth, 1)
        return int(value)

class TimestampCol(Column):
    """ Modified version of timestamp type """
    col_type = "timestamp"

    def __init__(self, ts_format, c_p_t, name, position, is_root, parents,
                 parentscount):
        super(TimestampCol, self).__init__(name, self.col_type, position,
                                           is_root, parents, parentscount)
        self.ts_format = ts_format
        self.c_p_t = c_p_t

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
            raise TypeError
        except ValueError:
            print "invalid value in ProbabilityDist"
            self.probability_dict = None
            raise ValueError
        else:
            self.probability_dict = probability_dict

class Tree(object):
    """ Manages an entire tree (collection of columns); Contains methods
    for printing data using tree"""

    def __init__(self, columns):
        self.columns = columns
