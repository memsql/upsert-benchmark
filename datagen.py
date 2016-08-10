# Data gen script
# Approximates a reasonable workload for the fast-update use case

import time
import string
import random
import sys
import pickle
from os.path import abspath, dirname, join
from numpy.random import pareto, permutation
from collections import namedtuple


def print_progress_of(iterable, char_width=80, frequency=100):
    n = len(iterable)
    i = 0
    for item in iterable:
        percent_done = (1.0 * i) / n
        bar_length = char_width - 2  # edges
        filled_length = int(round(percent_done * bar_length))
        unfilled_length = bar_length - filled_length
        if i % frequency == 0:
            sys.stdout.write('[%s%s]\r' % (filled_length * '#', unfilled_length * '-'))
            sys.stdout.flush()
        if i == n - 1:
            sys.stdout.write('[%s]\n' % (bar_length * '#'))
            sys.stdout.flush()
        yield item
        i += 1


def pareto_approximation(n):
    """ Approximates a pareto on a finite set of size n """
    shape = 3.  # arbitrarily chosen
    idx = n
    while idx >= n:
        rnd = pareto(shape)
        idx = int(n * (rnd / shape))
    return idx


def genericize(n):
    """ Fixes a permutation on n elements.

        Helps make generated data look more real,
        avoiding skew toward 0's """
    mapping = permutation(n)
    return lambda x : mapping[x]


def gen_subcustomer_id(letters, length):
    """ Generates a random integer and inteprets that
        as a number in base 26, mapping to a string """
    num_letters = 26
    total_possible = num_letters ** length
    rnd = pareto_approximation(total_possible)
    result = ''
    while rnd > 0:
        result += letters[rnd % num_letters]
        rnd /= num_letters
    if len(result) < length:
        result = result + letters[0] * (length - len(result))
    return result


def gen_ip_addrs(num):
    ips = []
    path = join(dirname(abspath(__file__)), 'ip_addrs.txt')
    with open(path, 'r') as f:
        lines = [line.split(',') for line in f.readlines()]
        for _ in xrange(num):
            lo, hi = random.choice(lines)
            lo = tuple(int(item) for item in lo.split('.'))
            hi = tuple(int(item) for item in hi.split('.'))
            ips.append('.'.join(str(random.randint(l, h)) for (l, h) in zip(lo, hi)))
    return ips


# namedtuples are pleasant and less memory-intensive
# than their equivalent dictionaries or lists
Row = namedtuple('Row', ['customer_code', 'timestamp_of_data',
    'subcustomer_id', 'geographic_region', 'billing_flag',
    'ip_address', 'bytes', 'hits'])


def main(scale_factor=100000):
    """ Customer_codes and subcustomer_ids are drawn from a
        pareto approximation. Every other column is drawn
        uniformly at random. """

    max_customer_code = 100000
    num_geographic_regions = 10
    num_billing_flags = 5
    max_hits = 50
    num_ip_addrs = 10000

    subcustomer_id_length = 12
    letters = permutation(list(string.uppercase))

    user_ips = gen_ip_addrs(num_ip_addrs)
    rand_ip = lambda : user_ips[pareto_approximation(num_ip_addrs)]

    genericize_customer = genericize(max_customer_code)
    rand_customer = lambda : genericize_customer(
        pareto_approximation(max_customer_code))

    byte_options = range(8192, 5000000, 1024)
    rand_bytes = lambda : byte_options[pareto_approximation(len(byte_options))]

    hit_options = range(50, 1000, 4)
    rand_hits = lambda : hit_options[pareto_approximation(len(hit_options))]

    rows = []

    for _ in print_progress_of(xrange(scale_factor)):

        row_customer_code = rand_customer()
        row_timestamp_of_data = int(1000 * time.time())
        row_subcustomer_id = gen_subcustomer_id(letters, subcustomer_id_length)
        row_geographic_region = random.randint(1, num_geographic_regions)
        row_billing_flag = random.randint(1, num_billing_flags)
        row_ip_address = rand_ip()
        row_bytes = rand_bytes()
        row_hits = rand_hits()

        row = Row(row_customer_code,
                  row_timestamp_of_data,
                  row_subcustomer_id,
                  row_geographic_region,
                  row_billing_flag,
                  row_ip_address,
                  row_bytes,
                  row_hits)

        rows.append(tuple(row))
    # rows.sort()

    path = join(dirname(abspath(__file__)), 'data')

    print('Serializing data to disk')
    with open(path, 'w') as f:
        pickle.dump(rows, f)


if __name__ == '__main__':
    main()
