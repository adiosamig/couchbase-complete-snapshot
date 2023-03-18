import sys
import json
import couchbase_meta
from tabulate import tabulate
import pandas as pd
import debug as debugPerformance
import logging
import traceback

def uniqueVersions(list1):
 
    # initialize a null list
    unique_list = []
 
    # traverse for all elements
    for x in list1:
        # check if exists in unique_list or not
        if x not in unique_list:
            unique_list.append(x)
    return unique_list

couchbaseServer=sys.argv[1]
couchbaseUser=sys.argv[2]
couchbaseSecret=sys.argv[3]

couchbaseEnvironment=couchbase_meta.couchbasePlatform(couchbaseServer,couchbaseUser,couchbaseSecret)
couchbaseEnvironment.getClusterName()
couchbaseEnvironment.getClusterVersion()
couchbaseEnvironment.getUsersOnCluster()
couchbaseEnvironment.getXdcrConnections()
couchbaseEnvironment.getNodesOnCluster()
couchbaseEnvironment.prepareBucketData()
couchbaseEnvironment.getSettings()
couchbaseEnvironment.getRebalance()

clusterNodes=couchbaseEnvironment.clusterNodes
clusterBuckets=couchbaseEnvironment.buckets
clusterRoles=couchbaseEnvironment.usersOnCluster
clusterSettings=couchbaseEnvironment.settingsCluster

dataFrameforNodes=pd.DataFrame(couchbaseEnvironment.clusterNodes)
dataFrameforBuckets=pd.DataFrame(couchbaseEnvironment.buckets)
dataFrameforRoles=pd.DataFrame(couchbaseEnvironment.usersOnCluster)
dataFrameforXdcr=pd.DataFrame(couchbaseEnvironment.xdcrConnections)
dataFrameFailover=pd.DataFrame(clusterSettings)


print("----- Cluster Nodes -----")
print(tabulate(dataFrameforNodes, headers = 'keys', tablefmt = 'psql'))
print("----- Cluster Buckets -----")
print(tabulate(dataFrameforBuckets, headers = 'keys', tablefmt = 'psql'))
print("----- Cluster XDCR -----")
print(tabulate(dataFrameforXdcr, headers = 'keys', tablefmt = 'psql'))
print("----- Cluster Roles -----")
print(tabulate(dataFrameforRoles, headers = 'keys', tablefmt = 'psql'))
print("----- Cluster Settings -----")
print(tabulate(dataFrameFailover, headers = 'keys', tablefmt = 'psql'))


# Evaluate Results

checkResults=[]
nodeVersions=[]

for node in clusterNodes:
    healtStatus=node.get('healtStatus')
    clusterMember=node.get('clusterMember')
    nodeServices=node.get('services')
    nodeVersion=node.get('couchbaseVersion')
    nodeVersions.append(nodeVersion)
    mdsControlCount=len(node.get('services'))
    if healtStatus!='healthy' or clusterMember!='active':
        checkModel={
            "problemStatement": 'Node is not available or joined cluster.',
            "problemArea": node.get('nodeIP'),
            "problemSeverity": 'Critical'
        }
        checkResults.append(checkModel)
    else:
        pass
    if mdsControlCount > 1:
        checkModel={
            "problemStatement": 'The node has multiple couchbase services.For production MDS model should be followed.',
            "problemArea": node.get('nodeIP'),
            "problemSeverity": 'Medium'
        }
        checkResults.append(checkModel)

uniqueCouchbaseVersions=uniqueVersions(nodeVersions)
if len(uniqueCouchbaseVersions) > 1:
    checkModel={
            "problemStatement": 'The node versions are different in the cluster.',
            "problemArea": 'Cluster',
            "problemSeverity": 'Critical'
    }
    checkResults.append(checkModel)
else:
    pass


for bucket in clusterBuckets:
    bucketReplica=bucket.get('bucketReplicas')
    vbucketCount=bucket.get('primaryVbucketCount')
    bucketResident=bucket.get('bucketResidentRatio')
    if bucketReplica==0:
        checkModel={
            "problemStatement": f''' {bucket.get('bucketName')} has no replica configured''',
            "problemArea": 'Bucket',
            "problemSeverity": 'Critical'
        }
        checkResults.append(checkModel)
    elif bucketReplica==3:
        checkModel={
            "problemStatement": f''' {bucket.get('bucketName')} has 3 replica configured''',
            "problemArea": 'Bucket',
            "problemSeverity": 'Warming'
        }
        checkResults.append(checkModel)
    if vbucketCount%1024!=0:
        checkModel={
            "problemStatement": f''' {bucket.get('bucketName')} has missing primary vbucket''',
            "problemArea": 'Bucket',
            "problemSeverity": 'Critical'
        }
        checkResults.append(checkModel)
    if bucketResident < 10:
        checkModel={
            "problemStatement": f''' {bucket.get('bucketName')} has low resident ratio''',
            "problemArea": 'Bucket',
            "problemSeverity": 'Critical'
        }
        checkResults.append(checkModel)


for setting in clusterSettings:
    if setting.get('configName')=='autofailover' and setting.get('status')==False:
        checkModel={
            "problemStatement": 'Auto failover configuration disabled',
            "problemArea": 'Cluster',
            "problemSeverity": 'Critical'
        }
        checkResults.append(checkModel)
    if setting.get('configName')=='email-alerting' and setting.get('status')==False:
        checkModel={
            "problemStatement": 'Email alerts are disabled',
            "problemArea": 'Cluster',
            "problemSeverity": 'Critical'
        }
        checkResults.append(checkModel)

dataFrameResults=pd.DataFrame(checkResults)
print("----- Check Notes -----")
print(tabulate(dataFrameResults, headers = 'keys', tablefmt = 'psql'))

print("----- Ping Test Results -----")
debugPerformance.getPingResults(couchbaseServer,couchbaseUser,couchbaseSecret)

print("----- Wait Cluster Test Results -----")
logging.basicConfig(filename='example.log',
                    filemode='w', 
                    level=logging.DEBUG,
                    format='%(levelname)s::%(asctime)s.%(msecs)03d::%(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S')
logger = logging.getLogger()

debugPerformance.waitCluster(couchbaseServer,couchbaseUser,couchbaseSecret,logger)


