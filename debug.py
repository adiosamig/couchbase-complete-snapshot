import logging
import traceback
from datetime import timedelta
import time

import couchbase
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import CouchbaseException
from couchbase.options import ClusterOptions, WaitUntilReadyOptions

def getPingResults(connectionIp,userName,userSecret):
    auth = PasswordAuthenticator(
        userName,
        userSecret,
    )
    
    cluster = Cluster(f'''couchbase://{connectionIp}''', ClusterOptions(auth))
    cluster.wait_until_ready(timedelta(seconds=5))
    ping_result = cluster.ping()
    return ping_result.as_json()

def waitCluster(connectionIp,userName,userSecret,logger):
    couchbase.configure_logging(logger.name, level=logger.level) 
    cluster = Cluster(f'''couchbase://{connectionIp}''',
                      ClusterOptions(PasswordAuthenticator(userName, userSecret)))
    for i in range(10):
        fo = open("example.log", "w")
        fo.truncate(0)
        time.sleep(1)
        print('-------------------------------')
        cluster.wait_until_ready(timedelta(seconds=3),
                                 WaitUntilReadyOptions(service_types=[ServiceType.KeyValue, ServiceType.Query]))

        logger.info('Cluster ready.')
        with open("example.log", "r") as file:
            first_line = file.readline()
            for last_line in file:
                pass
        startingMessage=f'''Starting Cocuhbase Cluster Connection: {first_line.split('::')[1]}'''
        finalMessage=f'''Couchbase Cluster is Ready To Serve:  {last_line.split('::')[1]}'''
        print(startingMessage)
        print(finalMessage)

    return True