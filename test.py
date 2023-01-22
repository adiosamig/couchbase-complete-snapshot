import requests
def getFailover():
    try:
        urlForHealth = f"http://172.17.0.4:8091/settings/alerts"
        getNodeDetails = requests.get(
            url=urlForHealth, auth=('Administrator', 'test123'))
        resultParsed = getNodeDetails.json()
        print(resultParsed)
    except Exception as couchbaseBucketException:
        print(couchbaseBucketException)


getFailover()