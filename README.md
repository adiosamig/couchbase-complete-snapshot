# Couchbase Complete Snapshot 

![](https://upload.wikimedia.org/wikipedia/commons/6/67/Couchbase%2C_Inc._official_logo.png)

Couchbase complete snapshot is responsible for collecting metrics from a single cluster and evaluating the results according to the **production best practices**


## Production Check List

1. In production couchbase clusters, MDS model should be applied for better scaling and stability.
2. Couchbase bucket should have at least 1 replica.
3. Couchbase bucket should have 1024 primary vbuckets.
4. A couchbase bucket's resident ratio needs to be high(It depends according to your bucket size, memory size etc..)
5. All nodes in the cluster needs to run with the same version of Couchbase.

