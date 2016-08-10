#### Log into MA
```
$ ssh admin@ec2-52-91-135-198.compute-1.amazonaws.com
```
or
```
$ ssh -i ~/.ssh/private_key.pem admin@ec2-52-91-135-198.compute-1.amazonaws.com
```

#### Create of file with the ips or hostname of the aggregators
```
$ memsql-ops agent-list | grep -v PRIMARY | awk '{print$2}' | grep '^[0-9]' > aggregators

```

### Copy private key to MA. This is required to ssh/scp to the aggregators.
```
$ scp ~/.ssh/private_key.pem admin@ec2-52-91-135-198.compute-1.amazonaws.com:~/.ssh/
```

### Get repo on MA

```
git clone https://github.com/memsql/upsert-benchmark.git ~/benchmark
```

### From the benchmark directory on the MA, edit the benchmark.cfg file
these credentials are used to scp and ssh to aggregators.

```
vi ~/benchmark/benchmark.cfg
```

```
[ssh]
username: admin
ssh_key: ~/.ssh/private_key.pem

```

### Run the setup script
This will ssh/scp to each aggregator and install dependencies

```
$ python ./setup.py -A aggregators
```

### Run the benchmark

```
$ ./benchmark.py -A aggregators
Getting hosts from file... aggregators
[##############################################################################]
Serializing data to disk
Copying files to aggregators
Running on aggregators
52,202,000 rows in total
5,220,200 rows per second
Min query latency: 0.063 ms
Max query latency: 166.189 ms
$
```

