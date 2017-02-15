from datetime import datetime
from datetime import timedelta
import numpy as np
from math import sqrt
from math import exp
from math import pi

def kernel_density_estimate(value_list,points=None):
    """ Returns KDE values and bandwidth for given list of numbers """
    # Find bandwidth
    # Use Silverman's rule of thumb to get bandwidth
    no_of_values = len(value_list)
    std_dev = np.std(value_list)
    iqr = get_quartile_range(value_list) # Interquartile range
    min_iqr_sd = min(std_dev,iqr)
    bandwidth = min_iqr_sd*((float(4)/(3*no_of_values))**(1.0/5))
    if bandwidth == 0:
        return 0,{sum(value_list)/no_of_values: 1}
    # Generate bins
    if points is None:
        bin_values = [int(v/bandwidth)*int(bandwidth)for v in value_list]
    else:
        bin_values = points
    # Get density for each bins
    densities = {}
    for _bin in bin_values:
        densities[_bin] = get_density(value_list,bandwidth,_bin)
    return bandwidth,densities

def get_density(value_list, stdev, x_val):
    """ Returns density of a bin based on given value list """
    prob_sum = 0
    for val in value_list:
        x_val_2 = float(x_val - val)/stdev
        prob_sum += exp(-0.5*(x_val_2**2))/(sqrt(2*pi)*stdev)
    prob_sum /= len(value_list)
    return prob_sum

def get_quartile_range(value_list):
    """ returns Interquartile Range (Q3 - Q1) of a given set of data points """
    #value_list = sorted(value_list)
    # Return the difference of Q3 and Q1 as IQR
    return np.subtract(*np.percentile(value_list, [75,25]))

def generate_dates(_start, _end):
    """Generates dates between given start and end """
    new_start = _start.replace(hour=0, minute=0, second=0, microsecond=0)
    new_end = _end.replace(hour=0, minute=0, second=0, microsecond=0)
    delta = new_end - new_start
    dates = []
    for d in range(delta.days + 1):
        dates.append(new_start + timedelta(days=d))
    return dates

def generate_hash_string(cols, parents_list):
    hash_string = str(cols[parents_list[0]])
    for parent in parents_list[1:]:
        hash_string += ";" + str(cols[parent])
    return hash_string

def extract_weekminute_probs(weektime_counts,weekday_counts):
    weektime_probs = {}
    for w_bucket in weektime_counts.keys():
            weektime_probs[w_bucket] = float(weektime_counts[w_bucket])/weekday_counts[int(w_bucket/(24*60))]
    return weektime_probs
