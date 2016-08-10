## Benchmark Numbers

Below are the results from both MemSQL and Cassandra running the benchmark script on different AWS M4 configurations. 

- M4 instances are the latest generation of General Purpose Instances
- 2.4 GHz Intel Xeon® E5-2676 v3 (Haswell) processors
- Balance of compute, memory, and network resources

##### Specs
- 40 vCPU per machine
- 160 GB Memory per machine

##### Benchmark Configuration
- The batch_size was set to 500 in this benchmark for both MemSQL and Cassandra
  - Increasing the batch size with MemSQL yielded significantly higher throughput
  - Testing with a batch size of 10,000 on AWS 10 x r3.8xlarge, we saw up to 7-8M rows per second
  - We could not get the Cassandra Python CQL driver to accept batch sizes larger than 500 without errors

##### MemSQL Configuration
- 1 aggregator and 2 leaves per host
- each leaf bound to a NUMA socket using `memsql-ops memsql-optimize`
- added `default_partitions_per_leaf = 1` to the `memsql.cnf` file on all leaves
  - easily done using `memsql-ops memsql-update-config --key default_partitions_per_leaf --value 1 --all`
  - restart the cluster after changing the setting using `memsql-ops memsql-restart --all`

##### Cassandra Configuration
- Because we are not Cassandra experts, we went with the default settings

### AWS: 10 x m4.10xlarge

#### MemSQL 
- 1 master aggregator, 9 child aggregators, and 20 leaves
```
$ ./benchmark.py -A cluster-nodes --workload-time=30
51,983,500 rows in total
5,198,350 rows per second
Min query latency: 0.055 ms
Max query latency: 172.780 ms
```

#### Cassandra
```
$ ./benchmark.py -c -A cluster-nodes --workload-time=30
14,039,000 rows in total
1,403,900 rows per second
Min query latency: 3.242 ms
Max query latency: 385.611 ms
```

### AWS: 5 x m4.10xlarge

#### MemSQL
- 1 master aggregator, 4 child aggregators, and 10 leaves
```
$ ./benchmark.py -A cluster-nodes --workload-time=30
80,167,000 rows in total
2,672,233 rows per second
```

#### Cassandra
```
$ ./benchmark.py -c -A cluster-nodes --workload-time=30
30,578,000 rows in total
1,019,266 rows per second
```


### AWS: 1 x m4.10xlarge

#### MemSQL
- 1 master aggregator, and 2 leaves
```
$ ./benchmark.py --workload-time=30
16,424,000 rows in total
547,466 rows per second
```

#### Cassandra
```
$ ./benchmark.py -c --workload-time=30
6,282,000 rows in total
209,400 rows per second
```