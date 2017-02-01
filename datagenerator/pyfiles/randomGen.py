import numpy
from numpy.random import random_sample
import math
import random

def poipdf(mean, x): #generates probability of x for given mean
    pro = (mean ** x) * math.exp(-mean) / math.factorial(x)
    return pro

def data_gen(values, probs, size): #generates list of length 'size' of elements in values for given probability
    bin = numpy.add.accumulate(probs)
    return values[numpy.digitize(random_sample(size), bin)]

def data_genNorm(values, probs, size): #generates list of length 'size' of elements in values for given probability
    total = round(sum(probs),8)
    probs2 = [float(round(p,8))/float(round(total,8)) for p in probs]
    bin = numpy.add.accumulate(probs2)
    index = numpy.digitize(random_sample(size), bin)[0]
    print 
    return values[index]

def interval_gen(rateParameter):
    return -math.log(1.0 - random.random()) / rateParameter

def minute_gen(number_of_events):
    # Function that generates a list of minutes based on the number of events
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
        #Probabilities are extracted by from surescript AD logs
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
