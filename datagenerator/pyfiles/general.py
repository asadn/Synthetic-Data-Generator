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
    if iqr == 0:
        min_iqr_sd = std_dev
    elif std_dev == 0:
        min_iqr_sd = float(iqr)/1.34
    else:
        min_iqr_sd = min(std_dev,float(iqr)/1.34)
    bandwidth = 0.55*min_iqr_sd*((no_of_values)**(-1.0/5))
    if bandwidth == 0:
        return 0,{sum(value_list)/no_of_values: 1}
    # Generate bins
    if points is None:
        bin_values = [int(v/bandwidth)*bandwidth for v in value_list]
    else:
        bin_values = points
    # Get density for each bins
    densities = {}
    bin_values = set(bin_values)
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
            if weekday_counts[int(w_bucket/(24*60))] != 0:
                weektime_probs[w_bucket] = float(weektime_counts[w_bucket])/weekday_counts[int(w_bucket/(24*60))]
            else:
                weektime_probs[w_bucket] = 0
    return weektime_probs
 
def bin_frequencies(value_list,points=None):
    """ Returns KDE values and bandwidth for given list of numbers """
    # Find bandwidth
    # Use Silverman's rule of thumb to get bandwidth
    densities = {}
    if(len(value_list)==1):
        densities[value_list[0]]=1
        return 0, densities
    no_of_values = len(value_list)
    std_dev = np.std(value_list)
    iqr = get_quartile_range(value_list) # Interquartile range
    min_iqr_sd = min(std_dev,iqr)
    bandwidth = 0.55*min_iqr_sd*((no_of_values)**(-1.0/5))
    if bandwidth == 0:
        return 0,{sum(value_list)/no_of_values: 1}
    # Generate bins
    if points is None:
        bin_values = [int(v/bandwidth)*bandwidth  for v in value_list]
    else:
        bin_values = points
    # Get density for each bins
    number_of_ele = len(bin_values)
    for _bin in bin_values:
        densities[_bin] = float(bin_values.count(_bin))/number_of_ele
    return bandwidth,densities


def minute_gen(number_of_events):
    """ Function that generates a list of minutes based on the number of events """
    no_of_mins = 0
    # For number of events less than 20, the number of minutes is
    # randomly picked from a range zero to number of minutes
    if(number_of_events <= 20):
        no_of_mins = numpy.random.choice(range(1,number_of_events+1),1)
    elif(number_of_events < 60):
        p20 = 0.957
        pOther = 0.043
        min_interval = data_gen([20,60],[p20,pOther],1)
        if(min_interval == 20):
            no_of_mins = numpy.random.choice(range(1,21),1)
        elif(min_interval == 60):
            #print(number_of_events)
            no_of_mins = numpy.random.choice(range(21,number_of_events+1),1)
            #print(no_of_mins)
    else:
        if(number_of_events < 100):
            p20 = 0.957
            p40 = 0.036
            p60 = 0.007
        elif(number_of_events < 1000):
            p20 = 0.7
            p40 = 0.181
            p60 = 0.119
        else:
            p20 = 0.146
            p40 = 0.146
            p60 = 0.708
        #Select the interval of number of minutes in an hour
        min_interval = data_gen([20,40,60],[p20,p40,p60],1)

        # Select the number of minutes in an hour from the interval
        if(min_interval == 20):
            no_of_mins = numpy.random.choice(range(1,21),1)
        elif(min_interval == 40):
            no_of_mins = numpy.random.choice(range(21,41),1)
        elif(min_interval == 60):
            no_of_mins = numpy.random.choice(range(41,61),1)

    # Generate n (number of events) minutes randomly uniformly from 0-59 minutes
    mins = numpy.random.uniform(0,59,no_of_mins)
    mins_array = list(numpy.random.choice(mins, number_of_events, replace = True))
    return mins_array

def get_weekday_count(min_date,max_date):
    total_days = (max_date - min_date).days+1
    min_wday = (min_date).weekday()
    weekday_counts = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}
    for i in range(0,total_days+1):
        if weekday_counts.has_key((i+min_wday)%7):
            weekday_counts[(i+min_wday)%7] += 1
        else:
            weekday_counts[(i+min_wday)%7] = 1
    return weekday_counts
