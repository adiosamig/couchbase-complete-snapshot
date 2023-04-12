import requests
import telnetlib
import sys
from prettytable import PrettyTable
import json

class couchbasePlatform:
    def __init__(self,hostName,loginInformation,loginSecret):
        self.hostname=hostName
        self.logininformation=loginInformation
        self.loginsecret=loginSecret
        self.clusterDefinition=''

    def getClusterVersion(self):
        try:
            urlForHealth = f"http://{self.hostname}:8091/pools"
            #print(self.hostname)
            getNodeDetails = requests.get(
                url=urlForHealth, auth=(self.logininformation, self.loginsecret))
            resultParsed = getNodeDetails.json()
            self.clusterVersion=resultParsed['implementationVersion']
        except Exception as couchbaseBucketException:
            print(couchbaseBucketException)
    def getNodesOnCluster(self):
        try:
            urlForHealth = f"http://{self.hostname}:8091/pools/nodes"
            getNodeDetails = requests.get(
                url=urlForHealth, auth=(self.logininformation, self.loginsecret))
            resultParsed = getNodeDetails.json()
            nodes=resultParsed.get('nodes')
            nodeList=[]
            for node in nodes:
                #print(node)
                clusterMember=node.get('clusterMembership')
                healtStatus=node.get('status')
                nodeIp=node.get('hostname')
                services=node.get('services')
                version=node.get('version')
                nodeModel={
                    "nodeIP": nodeIp,
                    "clusterMember":clusterMember,
                    "healtStatus": healtStatus,
                    "services" : services,
                    "couchbaseVersion":version
                }
                nodeList.append(nodeModel)
                self.clusterNodes=nodeList
        except Exception as couchbaseBucketException:
            print(couchbaseBucketException)
    def getUsersOnCluster(self):
        try:
            urlForHealth = f"http://{self.hostname}:8091/settings/rbac/users"
            getNodeDetails = requests.get(
                url=urlForHealth, auth=(self.logininformation, self.loginsecret))
            resultParsed = getNodeDetails.json()
            userList=[]
            for user in resultParsed:
                userModel={
                    "userName": user.get('name'),
                    "userRole": user.get('roles')
                }
                userList.append(userModel)
            self.usersOnCluster=userList
        except Exception as couchbaseBucketException:
            print(couchbaseBucketException)
    def getClusterName(self):
        try:
            urlForHealth = f"http://{self.hostname}:8091/pools/nodes"
            getNodeDetails = requests.get(
                url=urlForHealth, auth=(self.logininformation, self.loginsecret))
            resultParsed = getNodeDetails.json()
            self.clusterDefinition=resultParsed['clusterName']
        except Exception as couchbaseBucketException:
            print(couchbaseBucketException)
    def getRebalance(self):
        try:
            urlForHealth = f"http://{self.hostname}:8091/pools/default/pendingRetryRebalance"
            getNodeDetails = requests.get(
                url=urlForHealth, auth=(self.logininformation, self.loginsecret))
            resultParsed = getNodeDetails.json()
            #print(resultParsed)
            self.rebalanceStatus=resultParsed
        except Exception as couchbaseBucketException:
            print(couchbaseBucketException)
    def getXdcrConnections(self):
        try:
            urlForHealth = f"http://{self.hostname}:8091/pools/default/remoteClusters"
            getNodeDetails = requests.get(
                url=urlForHealth, auth=(self.logininformation, self.loginsecret))
            resultParsed = getNodeDetails.json()
            xdcrConnections=[]
            for remote in resultParsed:
                xdcrModel={
                    "xdcrName": remote.get('name'),
                    "xdcrConnectivity": remote.get('connectivityStatus'),
                    "targetNode":remote.get('hostname').split(":")[0]
                }
                xdcrConnections.append(xdcrModel)
            self.xdcrConnections=xdcrConnections
        except Exception as couchbaseBucketException:
            return couchbaseBucketException
    def prepareBucketData(self):
        try:
            bucketDetailReport=[]
            bucketsEndpoint = f"http://{self.hostname}:8091/pools/default/buckets"
            getBucketDetails = requests.get(
                url=bucketsEndpoint, auth=(self.logininformation,self.loginsecret))
            overallBucketData = getBucketDetails.json()
            bucketList=[]
            for bucket in overallBucketData:
                bucketList.append(bucket.get('name'))
                bucketName=bucket.get('name')
                vbucketMap=bucket.get('vBucketServerMap')
                vbucketCount=vbucketMap.get('vBucketMap')
                count=0
                for vbucket in vbucketCount:
                    count=count+1
                # call bucket endpoint with name and collect result
                bucketSpecialPoint = f"http://{self.hostname}:8091/pools/default/buckets/{bucketName}/stats"
                detailedBucket = requests.get(
                    url=bucketSpecialPoint, auth=(self.logininformation,self.loginsecret))
                statReport = detailedBucket.json()
                # collect details
                bucketStats=bucket.get('basicStats')
                avgResident = sum(statReport['op']['samples']['vb_active_resident_items_ratio'])/len(
                            statReport['op']['samples']['vb_active_resident_items_ratio'])
                bucketRecord = {
                    "bucketName": bucketName,
                    "primaryVbucketCount": count,
                    "bucketType": bucket.get('bucketType'),
                    "bucketReplicas": bucket.get('vBucketServerMap').get('numReplicas'),
                    "bucketQuotaPercentage": round(bucketStats.get('quotaPercentUsed'), 1),
                    "bucketItemCount":  round(bucketStats.get('itemCount')),
                    "bucketResidentRatio": avgResident,
                    "bucketDisUsedInMb": round((bucketStats.get("diskUsed"))/1024/1024, 1)
                }
                bucketDetailReport.append(bucketRecord)
            self.buckets=bucketDetailReport
        except Exception as couchbaseBucketException:
            print(couchbaseBucketException)
    def prepareIndexData(self):
        url = f''' http://{self.hostname}:8091/pools/default/nodeServices'''
        response = requests.get(url, auth=(self.logininformation, self.loginsecret))
        data = json.loads(response.content)
        indexData=[]
        # Check if the index service is enabled
        index_service_enabled = False
        for service in data.get('nodesExt'):
            for key in service.get('services').keys():
                if "index" in key:
                    index_service_enabled = True
                    break
        if index_service_enabled:
            # Set the endpoint URL for the indexes
            url = "http://127.0.0.1:8091/indexStatus"

            # Send the request and retrieve the response with authentication
            response = requests.get(url, auth=(self.logininformation, self.loginsecret))
            data = json.loads(response.content)
            indexList=data.get('indexes')

            # Print the indexes in table format
            if len(indexList) > 0:
                for index in indexList:
                    indexStatus=index['status']
                    indexDefinition=index['definition']
                    indexReplica=index['numReplica']
                    indexModel={
                        "indexDefinition" : indexDefinition,
                        "indexStatus": indexStatus,
                        "indexReplica": indexReplica
                    }
                    indexData.append(indexModel)
                self.indexes=indexData
            else:
                print('No indexes found.')
                self.indexes=[]
        else:
            print('Index service is not enabled.')
            self.indexes=[]
        return True
    

    def getSettings(self):
        try:
            urlForHealth = f"http://{self.hostname}:8091/settings/autoFailover"
            getNodeDetails = requests.get(
                url=urlForHealth, auth=(self.logininformation, self.loginsecret))
            resultParsed = getNodeDetails.json()
            settingsArray=[]
            settingModel={
                "configName": 'autofailover',
                "status": resultParsed.get('enabled'),
            }
            settingsArray.append(settingModel)
            urlForHealth = f"http://{self.hostname}:8091/settings/alerts"
            getNodeDetails = requests.get(
                url=urlForHealth, auth=(self.logininformation, self.loginsecret))
            resultParsed = getNodeDetails.json()
            settingModel={
                "configName": 'email-alerting',
                "status": resultParsed.get('enabled'),
            }
            settingsArray.append(settingModel)
            urlForHealth = f"http://{self.hostname}:8091/settings/autoCompaction"
            getNodeDetails = requests.get(
                url=urlForHealth, auth=(self.logininformation, self.loginsecret))
            resultParsed = getNodeDetails.json()
            settingModel={
                "configName": 'auto-compaction',
                "status": resultParsed.get('autoCompactionSettings').get('databaseFragmentationThreshold').get('percentage')
            }
            settingsArray.append(settingModel)
            self.settingsCluster=settingsArray
        except Exception as couchbaseBucketException:
            print(couchbaseBucketException)
    def checkExporters(self):
        nodeExporter=False
        try:
            conn = telnetlib.Telnet(self.hostname)
            response = self.hostname+' ' + '9120' +' - Success'
            nodeExporter=True
        except:
            response = self.hostname+' ' + '9120' +' - Failed'
        finally:
            return nodeExporter