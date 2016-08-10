# A Upsert Benchmark Script for MemSQL and Cassandra

This repository contains a simple benchmark to help compare performance of MemSQL and Cassandra on a fast-update workload.

## Getting started

Clone or download the repo onto the Master aggregator. You will need to rename the directory to benchmark.

```
git clone https://github.com/memsql/upsert-benchmark.git ~/benchmark
```

The script only requires a few dependencies:

* `python`
* `pip`
* `python-numpy`
* `cassandra-driver`

Note when installing manually, you must manually install all dependencies on the master and all child aggregators.

To install `numpy` and the `cassandra-driver` manually, run

```
sudo apt-get install python-numpy
sudo pip install cassandra-driver
```

Alternatively, you can run the setup.py command from the master aggregator. 

Note that the script will SSH itself over to the child aggregators and install dependencies, so you will need appropriate ssh and sudo permissions.

If using private a key to ssh, please specify username and key file location in the `benchmark.cfg` file. If credentials are not provided, the script will attempt password-less ssh as logged in user. If using password-less ssh as user other than logged user, specify only the username in the benchmark.cfg file. 

```
$ vi ~/benchmark/benchmark.cfg
```

```
[ssh]
username: admin
ssh_key: ~/.ssh/private.pem
```
Now, we can run the setup script.

```
python ./setup.py --aggregator='hostname1' --aggregator='hostname2'
```
where each hostname is the hostname of some child aggregator in your cluster.

Alternatively, you can specify a filename which contains a list of hosts.

```
python ./setup.py -A filename
```

## Usage

Ensure benchmark.py is executable.

```
chmod +x benchmark.py
```

### Local 

To run benchmark against a single MemSQL instance.

```
./benchmark.py
```

Provide the -c or --cassandra flag to run against Cassandra. 
 
```
./benchmark.py -c
```
 
The --cluster-memory and --workload-time flags may also be of interest. The benchmark will run over the generated dataset multiple times, if need be. The number of rows that are attempted to be added is a function of the cluster-memory flag.

### Distributed

```
./benchmark.py --aggregator=hostname1 --aggregator=hostname2
```

The script expects to be run on the master aggregator. If there are child aggregators that you also want to utilize for upserts, you can provide their hostnames via the command line. (`--aggregator=hostname`). You must repeat this flag for each child aggregator you'd like to use.

Note that the script will SCP itself over to the child aggregators, so you will need appropriate ssh permissions.

Again, the `-A` flag can be used to specify a list of hosts that you wish to run the benchmark on.

```
./benchmark.py -A filename
```

For additional information on the other flags available to the script, run

### Help
```
./benchmark.py --help
```

to see the help message.

## Running Cassandra

If you'd like to get a multiple node Cassandra cluster set up, the `cassandra-setup.sh` script can help. If you previously ran the `setup.py` script, it should have copied over the `cassandra-setup.sh` script to each of the nodes in your cluster. If not, you will need to manually setup each node in your Cassandra cluster.

The script will install the Oracle Java8 JDK (which requires a license agreement) and other dependencies. Simply run:

```
chmod +x cassandra-setup.sh
./cassandra-setup.sh
```

and accept the license agreement. You'll have to run this on every node in your cluster. 

After everything is installed, you'll need to change a couple of things in `apache-cassandra-[version]/conf/cassandra.yaml`. 

Change the `listen_address` from `localhost` to the internal cluster IP address for each machine. For every machine in the cluster, this line will be different.

```
listen_address: 10.0.3.230
# listen_interface: eth0
# listen_interface_prefer_ipv6: false

```
Next, change the `seeds` from `127.0.0.1...` to a list of all the internal IP addresses you'd like to use in the cluster. For every machine in the cluster, this line will be identical. 

```
# seeds is actually a comma-delimited list of addresses.
# Ex: "<ip1>,<ip2>,<ip3>"
- seeds: "127.0.0.1,..."
```

Cassandra does not dynamically reload these config files, so after changing the `cassandra.yaml` file, kill all the cassandra processes and relaunch. The cassandra binary is located at `apache-cassandra-[version]/bin/cassandra`.

```
./cassandra

```
