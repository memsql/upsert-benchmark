#!/usr/bin/env python

import os
import cPickle as pickle
import datagen
import multiprocessing
import shlex
import socket
import subprocess
import sys
import threading
import time
import ConfigParser

from optparse import OptionParser
from os.path import abspath, expanduser, isfile, dirname, join
from collections import namedtuple

from memsql.common import database

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement, BatchType

NUM_WORKERS = multiprocessing.cpu_count()
VERBOSE = False

Config = ConfigParser.ConfigParser()
Config.read('benchmark.cfg')

if sys.version_info.major == 3:
    xrange = range


Row = namedtuple('Row', ['customer_code', 'timestamp_of_data',
    'subcustomer_id', 'geographic_region', 'billing_flag',
    'ip_address', 'bytes', 'hits'])


def vprint(args):
    if VERBOSE:
        print(args)


class Timer(object):
    def __enter__(self): 
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start



def get_connection(options, db=''):
    """ Returns a new connection to the database. """
    if options.use_cassandra:
        cluster = Cluster()
        session = cluster.connect()
        setattr(session, "query", lambda s : session.execute(s + ';'))
        return session
    else:
        return database.connect(host=options.host, port=options.port,
                                user=options.user, database=db)

def benchmark_config(section):
    config = {}
    options = Config.options(section)
    for option in options:
        try:
            config[option] = Config.get(section, option)
        except:
            print("%s:" % option)
            config[option] = None
    return config


def parse_args():
    """ Argument parsing. """
    parser = OptionParser()
    parser.add_option("-d", "--database", default='perfdb')
    parser.add_option("-t", "--table", default='records')
    parser.add_option("--host", default='127.0.0.1')
    parser.add_option("--user", default='root')
    parser.add_option("-p", "--port", default=3306)
    parser.add_option("--data-file", default=expanduser('~/benchmark/data'),
                      help='data file to read from')
    parser.add_option("--workload-time", default=10)
    parser.add_option("-a", "--aggregator", action="append", dest="aggregators",
                      help=("provide aggregators to run on. if none are "
                            "provided, the script runs locally"),
                      default=[])
    parser.add_option("-A", "--aggregators-file", 
                      default=[], help='aggregators file to read from',  dest="aggfile")
                      
    parser.add_option("--batch-size", default=500)
    parser.add_option("--no-setup", action="store_true", default=False)
    parser.add_option("--mode", choices=["master", "child"],
                      default="master")
    parser.add_option("-c", "--cassandra", dest="use_cassandra",
                      action="store_true", default=False)
    parser.add_option("--drop-database", action="store_true", default=False)
    parser.add_option("--cluster-memory", default=1,  # gigabytes
                      help=("How much total memory the cluster has. The "
                            "number of attempted rows to be inserted is a "
                            "function of this"))
    parser.add_option("-v", "--verbose", action="store_true", default=False)
    (options, args) = parser.parse_args()
    global VERBOSE
    VERBOSE = options.verbose
    try:
        options.workload_time = int(options.workload_time)
    except TypeError:
        sys.stderr.write('workload-time must be an integer')
        exit(1)
    return options


def setup_perf_ks(options):
    with get_connection(options) as conn:
        vprint('Creating keyspace %s' % options.database)
        conn.query(("create keyspace if not exists %s with REPLICATION = "
                      "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } "
                      "and durable_writes = true" % options.database))
        conn.query('use %s' % options.database)
        create_cmd = ('create table if not exists %s ('
                      'customer_code int, timestamp_of_data timestamp,'
                      'subcustomer_id varchar, geographic_region int,'
                      'billing_flag int, ip_address varchar,'
                      'hits counter, primary key (timestamp_of_data, '
                      'customer_code, subcustomer_id, geographic_region, '
                      'billing_flag, ip_address))') % options.table

        conn.query(create_cmd)


def setup_perf_db(options):
    """ Create a database and table for this benchmark to use. """

    with get_connection(options, db='information_schema') as conn:
        vprint('Creating database %s' % options.database)
        conn.query('create database if not exists %s partitions 20' % options.database)
        conn.query('use %s' % options.database)
        conn.query('set global multistatement_transactions = 0')

        vprint('Creating table %s' % options.table)

        create_cmd = ('create table if not exists %s ('
                      'customer_code int unsigned not null, '
                      'timestamp_of_data timestamp default current_timestamp, '
                      'subcustomer_id char(12), '
                      'geographic_region int unsigned not null, '
                      'billing_flag int unsigned not null, '
                      'ip_address char(20), '
                      'bytes bigint unsigned not null, '
                      'hits bigint unsigned not null, '
                      'primary key (timestamp_of_data, customer_code, '
                      'subcustomer_id, geographic_region, billing_flag, '
                      'ip_address))') % options.table

        conn.query(create_cmd)


def setup(options):
    if not options.use_cassandra:
        setup_perf_db(options)
    else:
        setup_perf_ks(options)

def get_aggregators(options):
    print('Getting hosts from file... %s' % options.aggfile)
    path = join(dirname(abspath(__file__)), options.aggfile)
    with open(path, 'r') as f:
        for line in f:
            options.aggregators.append(line)
    return options.aggregators
    

def convert_cluster_mem_to_num_rows(options):
    """ Converts the command line arg cluster-memory into
        the number of rows to upsert. cluster-memory is given
        in gigabytes. """
    mem_bytes = int(float(options.cluster_memory) * (1024 ** 3))
    sample_row = Row(customer_code=56779,
                     timestamp_of_data=1468286020962,
                     subcustomer_id='GMMNLZNRUNEA',
                     geographic_region=3,
                     billing_flag=3,
                     ip_address='119.163.9.26',
                     bytes=1332000, hits=31)
    estimated_db_mem = 136  # discussed offline
    cost_per_row = sys.getsizeof(sample_row) + estimated_db_mem
    num_rows = (mem_bytes / cost_per_row) / 2
    num_machines = len(options.aggregators) + 1
    return num_rows / num_machines


def generate_data_file(options):
    num_rows = convert_cluster_mem_to_num_rows(options)
    vprint('Generating test data: {:,} rows'.format(num_rows))
    if isfile(options.data_file):
        vprint('Using existing data file: %s' % options.data_file)
        return
    datagen.main(num_rows)


class Analytics(object):
    def __init__(self):
        self.upsert_counts = [0 for _ in xrange(NUM_WORKERS)]
        self.latency_totals = [0 for _ in xrange(NUM_WORKERS)]
        self.latency_mins = [float("infinity") for _ in xrange(NUM_WORKERS)]
        self.latency_maxs = [0 for _ in xrange(NUM_WORKERS)]
        self.start_time = time.time()
        self.last_reported_time = time.time()
        self.last_reported_count = 0
        self.num_records = 0
        self.report_frequency = 100

    def record(self, batch_size, thread_id, latency):
        self.upsert_counts[thread_id] += batch_size
        self.latency_totals[thread_id] += latency
        self.latency_mins[thread_id] = min(latency, self.latency_mins[thread_id])
        self.latency_maxs[thread_id] = max(latency, self.latency_maxs[thread_id])
        self.num_records += 1
        if self.num_records % self.report_frequency == 0:
            self.continuous_report()

    def continuous_report(self):
        interval = (time.time() - self.last_reported_time)
        self.last_reported_time = time.time()
        cur_total = sum(self.upsert_counts)
        total = cur_total - self.last_reported_count
        self.last_reported_count = cur_total
        sys.stdout.write('Current upsert throughput: %d rows / s\n' % (total / interval))
        sys.stdout.flush()

    def update_min(self, latency):
        # Min is associative, so taking first element is fine
        # We only care about the min across the cluster anyway
        self.latency_mins[0] = min(latency, self.latency_mins[0])

    def update_max(self, latency):
        self.latency_maxs[0] = max(latency, self.latency_maxs[0])

    def update_totals(self, latency):
        self.latency_totals[0] += latency


ANALYTICS = Analytics()


class InsertWorker(threading.Thread):
    """ A simple thread which inserts generated data in a loop. """

    def __init__(self, stopping, upserts, thread_id, batch_size):
        super(InsertWorker, self).__init__()
        self.stopping = stopping
        self.daemon = True
        self.exception = None
        self.upserts = upserts
        self.num_distinct_queries = len(self.upserts)
        self.options = options
        self.thread_id = thread_id
        self.batch_size = batch_size

    def run(self):
        # This is a hot path. conn.execute releases the GIL,
        # but everything else holds it. The work done outside
        # of the conn.execute call should be minimized, else python
        # becomes the bottleneck of the benchmark.
        count = 0
        batch_size = self.batch_size
        with get_connection(options, db=self.options.database) as conn:
            query_idx = 0
            while (not self.stopping.is_set()):
                with Timer() as t:
                    conn.execute(self.upserts[query_idx])
                ANALYTICS.record(batch_size, self.thread_id, t.interval)
                query_idx = (query_idx + 1) % len(self.upserts)
                count += batch_size
        if self.thread_id == 1:
            print('')


def warmup(options):
    vprint('Warming up workload')
    if options.use_cassandra: return
    with get_connection(options, db=options.database) as conn:
        conn.execute('show tables;')
        conn.execute('set global multistatement_transactions = 0;')


def get_cassandra_queries(options, batch_size):

    with open(options.data_file, 'r') as f:
        print('Deserializing data')
        rows = [Row(*t) for t in pickle.load(f)]

    prefix = 'update %s.%s set hits = hits + 1 ' % (options.database, options.table)

    primary_key_cols = ['timestamp_of_data', 'customer_code', 'subcustomer_id',
                        'geographic_region', 'billing_flag', 'ip_address']

    updates = []
    for row in rows:
        update = prefix + 'where ' + ' and '.join(['%s=%r' %
            (name, getattr(row, name)) for name in primary_key_cols]) + ';'
        updates.append(update)

    batches = [updates[i:i+batch_size] for i in xrange(0, len(updates), batch_size)]

    batch_objects = []
    for batch in batches:
        batch_object = BatchStatement(batch_type=BatchType.COUNTER)
        for update in batch:
            batch_object.add(SimpleStatement(update))
        batch_objects.append(batch_object)

    return batch_objects


def format_row(row):
    return '(%r, %r, %r, %r, %r, %r, %r)' % (
        row.customer_code,
        row.subcustomer_id,
        row.geographic_region,
        row.billing_flag,
        row.ip_address,
        row.bytes,
        row.hits
    )


def format_query(options, rows):
    prefix = ('insert into %s (customer_code, subcustomer_id, '
              'geographic_region, billing_flag, ip_address, bytes, hits) '
              'values ') % options.table

    postfix = (' on duplicate key update bytes = values(bytes) + bytes, '
               'hits = values(hits) + hits')

    return prefix + ','.join([format_row(row) for row in rows]) + postfix


def get_queries(options, batch_size):

    with open(options.data_file, 'r') as f:
        rows = []
        print('Deserializing data')
        rows = [format_row(Row(*row)) for row in pickle.load(f)]

    batches = [rows[i:i+batch_size] for i in xrange(0, len(rows), batch_size)]
    for batch in batches:
        batch.sort()

    prefix = ('insert into %s (customer_code, subcustomer_id, '
              'geographic_region, billing_flag, ip_address, bytes, hits) '
              'values ') % options.table

    postfix = (' on duplicate key update bytes = values(bytes) + bytes, '
               'hits = values(hits) + hits')

    return [prefix + ','.join(batch) + postfix for batch in batches]


def on_master_agg(options):
    return options.mode == 'master'


def run_benchmark(options):
    """ Run a set of InsertWorkers and record their performance. """

    batch_size = 500
    if options.use_cassandra:
        upserts = get_cassandra_queries(options, batch_size)
    else:
        upserts = get_queries(options, batch_size)

    stopping = threading.Event()
    workers = [InsertWorker(stopping, upserts[i::NUM_WORKERS], i, batch_size)
               for i in xrange(NUM_WORKERS)]

    print('Launching %d workers with batch size of %d' % (NUM_WORKERS, batch_size))

    [worker.start() for worker in workers]
    time.sleep(options.workload_time)

    vprint('Stopping workload')

    stopping.set()
    [worker.join() for worker in workers]


def cleanup(options):
    """ Cleanup the database this benchmark is using. """

    with get_connection(options, db=options.database) as conn:
        if not options.use_cassandra:
            conn.query('drop database %s' % options.database)
        else:
            conn.query('drop keyspace %s' % options.database)


def hostport_from_aggregator(options, aggregator):
    if ":" in aggregator:
        h, p = aggregator.split(":")
        return h, int(p)
    else:
        return aggregator, options.port

def scp_myself_to_all_aggs(options):
    print "Copying files to aggregators"
    for aggregator in options.aggregators:
        agg_host, agg_port = hostport_from_aggregator(options, aggregator.strip())

        ssh_user = benchmark_config("ssh")['username']
        ssh_key = benchmark_config("ssh")['ssh_key']
        benchmark_path = os.path.expanduser(os.path.join("~", "benchmark"))
        
        if ssh_user and ssh_key:
            
            uri = '%s@%s' % (ssh_user, agg_host)
            remote_cmd = ':'.join([uri,benchmark_path])
            
            # If you have password-less ssh, you shouldn't need some of these args
            ssh_args = '-i %s' % expanduser(ssh_key)
            ssh_args += ' -o StrictHostKeyChecking=no'
            
            # mkdir on aggregator if it does not exist
            mkdir_cmd = 'ssh %s %s@%s mkdir -p %s' % \
                (ssh_args, ssh_user, agg_host, benchmark_path)
            subprocess.Popen(shlex.split(mkdir_cmd)).wait()
            
            copy_cmd = 'scp %s %%s %s' % (ssh_args, remote_cmd)
            
        elif not ssh_key and ssh_user:
            print "Trying SCP as %s with password-less ssh" % ssh_user
            uri = '%s@%s:%s' % (ssh_user, agg_host, benchmark_path)
            
            # mkdir on aggregator if it does not exist
            mkdir_cmd = 'ssh %s@%s mkdir -p %s' % \
                (ssh_user, agg_host, benchmark_path)
            subprocess.Popen(shlex.split(mkdir_cmd)).wait()
            # setup command to copy files to aggregator
            copy_cmd = 'scp %%s %s' % (uri)
        else:
            # if no username was specified in the config file, try logged in user
            print "Trying SCP as logged in user..."
            username = os.getlogin()
            
            uri = '%s@%s:%s' % \
                (username, agg_host, benchmark_path)
            
            # mkdir on aggregator if it does not exist
            mkdir_cmd = 'ssh %s@%s mkdir -p %s' % \
            (username, agg_host, benchmark_path)
            subprocess.Popen(shlex.split(mkdir_cmd)).wait()
            # setup command to copy files to aggregator
            copy_cmd = 'scp %%s %s' % (uri)

        # Copy python scripts to all aggregators
        
        for f in [options.data_file, abspath(__file__), abspath(datagen.__file__)]:
            cmd = shlex.split(copy_cmd % expanduser(f))
            subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()


def run_on_all_aggs(options):
    processes = []
    print "Running on aggregators"
    
    for aggregator in ['localhost'] + options.aggregators:
        agg_host, agg_port = hostport_from_aggregator(options, aggregator)
        
        # Get ssh config info
        ssh_user = benchmark_config("ssh")['username']
        ssh_key = benchmark_config("ssh")['ssh_key']

        remote_cmd = 'nohup python %s --mode=child' % abspath(__file__)
        remote_cmd += ' -c' if options.use_cassandra else ''
        remote_cmd += ' --database=%s' % options.database
        remote_cmd += ' --table=%s' % options.table
        remote_cmd += ' --port=%s' % agg_port
        remote_cmd += ' --data-file=%s' % options.data_file
        remote_cmd += ' --workload-time=%s' % options.workload_time
        remote_cmd += ' --cluster-memory=%s' % options.cluster_memory

        if ssh_user and ssh_key:
            # If you have password-less ssh, you shouldn't need these
            ssh_args = '-i %s' % expanduser(ssh_key)
            ssh_args += ' -o StrictHostKeyChecking=no'
            user = '%s@' % ssh_user if agg_host != 'localhost' else ''
            run_cmd = 'ssh %s %s%s %s' % \
                (ssh_args, user, agg_host, remote_cmd)
        elif not ssh_key and ssh_user:
            print "Trying with password-less ssh as %s" % ssh_user
            user = '%s@' % ssh_user if agg_host != 'localhost' else ''
            run_cmd = 'ssh %s%s %s' % \
                (user, agg_host, remote_cmd)
        else:
            # if no username was specified in the config file, try logged in user
            print "Trying as logged in user..."
            username = os.getlogin()
            
            user = '%s@' % username if agg_host != 'localhost' else ''
            run_cmd = 'ssh %s%s %s' % \
                (user, agg_host, remote_cmd)
        
        processes.append(subprocess.Popen(shlex.split(run_cmd),
                         stdout=subprocess.PIPE,
                         bufsize=1))
    return processes


def report(options, child_aggs_total=0):
    count = sum(ANALYTICS.upsert_counts)
    total_count = count + child_aggs_total
    total_latency = sum(ANALYTICS.latency_totals)
    min_latency = min(ANALYTICS.latency_mins)
    max_latency = max(ANALYTICS.latency_maxs)

    print('{:,} rows in total'.format(total_count))
    print("{:,} rows per second".format(total_count / options.workload_time))
    print('Min query latency: %.3f ms' % (1000 * min_latency))
    print('Max query latency: %.3f ms' % (1000 * max_latency))


def child_agg_report(options):
    count = sum(ANALYTICS.upsert_counts)
    print('%s inserted %s rows' % (socket.gethostname(), count))
    total_latency = sum(ANALYTICS.latency_totals)
    min_latency = min(ANALYTICS.latency_mins)
    max_latency = max(ANALYTICS.latency_maxs)

    print('{:,} rows in total'.format(count))
    print("{:,} rows per second".format(count / options.workload_time))
    print('Min query latency: %f s' % (min_latency))
    print('Max query latency: %f s' % (max_latency))

def master_aggregator_main(options):
    try:
        if not options.no_setup:
            setup(options)
            warmup(options)
            generate_data_file(options)

        if options.aggregators:
            if not options.no_setup:
                vprint('Distributing files to all machines')
                scp_myself_to_all_aggs(options)
            child_aggs_total = 0
            processes = run_on_all_aggs(options)
            time.sleep(5)  # network latency

            def extract_upsert(line):
                return int(line.strip().split(' ')[-4])

            def extract_latency(line):
                return float(line.strip().split(' ')[-2])

            # Read current upsert speed and final statistics
            # from all aggregators
            num_done = 0
            while num_done < len(options.aggregators) + 1:
                cur_upsert_rates = []
                for proc in processes:
                    line = proc.stdout.readline()
                    if line.strip().endswith('rows / s'):
                        cur_upsert_rates.append(extract_upsert(line))
                    if line.strip().endswith('rows') and 'inserted' in line:
                        child_aggs_total += int(line.split(' ')[-2])
                    if 'Min query latency' in line:
                        ANALYTICS.update_min(extract_latency(line))
                    if 'Max query latency' in line:
                        ANALYTICS.update_max(extract_latency(line))
                        num_done += 1
                if sum(cur_upsert_rates) > 0:
                    sys.stdout.write('Current upsert: {:,} rows per sec\r'.format(sum(cur_upsert_rates)))
            print('')

            [p.wait() for p in processes]
            report(options, child_aggs_total=child_aggs_total)
        else:
            run_benchmark(options)
            report(options)
    except KeyboardInterrupt:
        print("Interrupted... exiting...")
    finally:
        if options.drop_database:
            cleanup(options)


def child_aggregator_main(options):
    if not options.no_setup:
        generate_data_file(options)
        warmup(options)
    run_benchmark(options)
    child_agg_report(options)


if __name__ == '__main__':
    options = parse_args()
    if options.aggfile:
        get_aggregators(options)
        
    if "master" == options.mode:
        master_aggregator_main(options)
    elif "child" == options.mode:
        child_aggregator_main(options)
    else:
        exit(1)
