# Couchbase Complete Snapshot 

![](https://upload.wikimedia.org/wikipedia/commons/6/67/Couchbase%2C_Inc._official_logo.png)

Couchbase complete snapshot is responsible for collecting metrics from a single cluster and evaluating the results according to the **production best practices**


## Production Check List

### Cluster Health Check List

#### Architecture
- MDS model should be applied for better scaling and stability.
- Couchbase version should be the same among the nodes in the cluster.

#### Buckets

- Couchbase bucket should have at least 1 replica.
- Couchbase bucket should have 1024 primary vbuckets.
- A couchbase bucket's resident ratio needs to be high(It depends according to your bucket size, memory size etc..)

#### Settings

- Auto failover configuration needs to be enabled.
- Email alerts needs to be enabled.

### XDCR Health Check List

- XDCR cluster versions should be the with the production cluster.

## Installation and Running

1. Clone the repository

```bash
https://github.com/adiosamig/couchbase-complete-snapshot.git
```

2. Run the following command in repo's directory.

```python
python3 couchbase-snapshot.py 127.0.0.1 user_name password
```
Example results

```
----- Cluster Nodes -----
+----+-----------------+-----------------+---------------+-----------------+-----------------------+
|    | nodeIP          | clusterMember   | healtStatus   | services        | couchbaseVersion      |
|----+-----------------+-----------------+---------------+-----------------+-----------------------|
|  0 | 172.17.0.2:8091 | active          | healthy       | ['kv']          | 7.1.3-3479-enterprise |
|  1 | 172.17.0.3:8091 | active          | healthy       | ['kv']          | 7.1.3-3479-enterprise |
|  2 | 172.17.0.4:8091 | active          | healthy       | ['kv']          | 7.1.3-3479-enterprise |
|  3 | 172.17.0.5:8091 | active          | healthy       | ['index', 'kv'] | 7.1.3-3479-enterprise |
+----+-----------------+-----------------+---------------+-----------------+-----------------------+
----- Cluster Buckets -----
+----+--------------+-----------------------+--------------+------------------+-------------------------+-------------------+-----------------------+---------------------+
|    | bucketName   |   primaryVbucketCount | bucketType   |   bucketReplicas |   bucketQuotaPercentage |   bucketItemCount |   bucketResidentRatio |   bucketDisUsedInMb |
|----+--------------+-----------------------+--------------+------------------+-------------------------+-------------------+-----------------------+---------------------|
|  0 | beer-sample  |                  1024 | membase      |                1 |                     7.8 |              7303 |                   100 |                48.2 |
+----+--------------+-----------------------+--------------+------------------+-------------------------+-------------------+-----------------------+---------------------+
----- Cluster XDCR -----
+----+------------+--------------------+--------------+
|    | xdcrName   | xdcrConnectivity   | targetNode   |
|----+------------+--------------------+--------------|
|  0 | demo       | RC_OK              | 172.17.0.6   |
|  1 | demo_2     | RC_OK              | 172.17.0.7   |
+----+------------+--------------------+--------------+
----- Cluster Roles -----

----- Cluster Settings -----
+----+-----------------+----------+
|    | configName      |   status |
|----+-----------------+----------|
|  0 | autofailover    |     True |
|  1 | email-alerting  |    False |
|  2 | auto-compaction |       30 |
+----+-----------------+----------+
Good
----- Check Notes -----
+----+--------------------------------------------------------------------------------------------------------------------+-------------------+-------------------+
|    | problemStatement                                                                                                   | problemArea       | problemSeverity   |
|----+--------------------------------------------------------------------------------------------------------------------+-------------------+-------------------|
|  0 | XDCR and Production cluster versions are different                                                                 | 172.17.0.7 - XDCR | Critical          |
|  1 | The node has multiple couchbase services.For production MDS model should be followed.                              | 172.17.0.5:8091   | Medium            |
|  2 | Email alerts are disabled                                                                                          | Cluster           | Critical          |
|  3 | Default node exporter port can not be reached.If node exporter port is different from default ignore this problem. | Monitoring        | Medium            |
+----+--------------------------------------------------------------------------------------------------------------------+-------------------+-------------------+
----- Ping Test Results -----
+----+-----------------+-------------+---------------+
|    | nodeIp          | pingState   |   latency(us) |
|----+-----------------+-------------+---------------|
|  0 | 172.17.0.2:8091 | ok          |           615 |
|  1 | 172.17.0.3:8091 | ok          |           577 |
|  2 | 172.17.0.4:8091 | ok          |           550 |
|  3 | 172.17.0.5:8091 | ok          |           648 |
+----+-----------------+-------------+---------------+
```


## Supported Couchbase Version

- Couchbase 7.0.X
- Couchbase 7.1.X

