import requests
import json

javaHome = "/usr/java/jdk1.8.0_112/"
hdpVersion = "2.6.5"
hadoopNameNode = "10.0.3.1:9000"
hadoopResourceManager = "http://10.0.3.15:8088/ws/v1"
hadoopWebhdfsHost = "http://10.0.3.1:50070/webhdfs/v1"
validateKnoxSSL = False
useKnoxGateway = False

lzoJar = {
    "2.3.2.0-2950": "",
    "2.6.5": "/usr/hdp/2.4.0.0-169/hadoop/lib/hadoop-lzo-0.6.0.2.4.0.0-169.jar"
}

username = ""
password = ""


def createHdfsPath(path):
    return "hdfs://" + hadoopNameNode + path


def webhdfsGetRequest(path, op, allow_redirects=False):
    url = hadoopWebhdfsHost + path
    print url
    response = requests.get("%s?op=%s" % (url, op), allow_redirects=allow_redirects, verify=validateKnoxSSL,
                            auth=(username, password))
    print ">>> Status: %d (%s)" % (response.status_code, url)
    return response.json()


def webhdfsPutRequest(path, op, allow_redirects=False):
    url = hadoopWebhdfsHost + path
    response = requests.put("%s?op=%s" % (url, op), "", allow_redirects=allow_redirects, verify=validateKnoxSSL,
                            auth=(username, password))
    print ">>> Status: %d (%s)" % (response.status_code, url)
    return response


def pathExists(path):
    response = webhdfsGetRequest(path, "GETFILESTATUS")
    return (response.has_key("FileStatus"), response)


def createDir(path):
    response = webhdfsPutRequest(path, "MKDIRS").json()
    return (response.has_key("boolean") and response["boolean"], response)


def uploadFile(localFile, remoteFile):
    response = webhdfsPutRequest(remoteFile, "CREATE&overwrite=true")
    location = response.headers.get("Location")
    if location:
        with open(localFile, "rb") as fd:
            response = requests.put(location, fd, verify=validateKnoxSSL, auth=(username, password))
            print ">>> Status: %d (%s)" % (response.status_code, "<redirect>")
            return (True, response.text)
    return (False, "")


def createCacheValue(path, size, timestamp):
    return {
        "resource": createHdfsPath(path),
        "type": "FILE",
        "visibility": "APPLICATION",
        "size": size,
        "timestamp": timestamp
    }


def createNewApplication():
    url = hadoopResourceManager + "/cluster/apps/new-application"
    response = requests.post(url, "", verify=validateKnoxSSL, auth=(username, password))
    print ">>> Status: %d (%s)" % (response.status_code, url)
    return (True, response.json())


def submitSparkJob(sparkJson):
    url = hadoopResourceManager + "/cluster/apps"
    response = requests.post(url, sparkJson, headers={"Content-Type": "application/json"}, verify=validateKnoxSSL,
                             auth=(username, password))
    print ">>> Status: %d (%s)" % (response.status_code, url)
    return response


def start_program(projectFolder, remoteSparkJar, remoteAppJar, remoteSparkProperties, appName):
    if not pathExists(projectFolder):
        ret = createDir(projectFolder)
        if not ret[0]: raise Exception(json.dumps(ret[1]))
    ret = pathExists(remoteSparkJar)
    if not ret[0]: raise Exception(ret[1])
    sparkJarFileStatus = ret[1]["FileStatus"]
    ret = pathExists(remoteAppJar)
    if not ret[0]: raise Exception(ret[1])
    appJarFileStatus = ret[1]["FileStatus"]
    ret = pathExists(remoteSparkProperties)
    if not ret[0]: raise Exception(ret[1])
    sparkPropertiesFileStatus = ret[1]["FileStatus"]
    newApp = createNewApplication()

    sparkJob = {
        "application-id": newApp[1]["application-id"],
        "application-name": appName,
        "am-container-spec":
            {
                "local-resources":
                    {
                        "entry": [
                            {
                                "key": "__spark__.jar",
                                "value": createCacheValue(remoteSparkJar, sparkJarFileStatus["length"],
                                                          sparkJarFileStatus["modificationTime"])
                            },
                            {
                                "key": "__app__.jar",
                                "value": createCacheValue(remoteAppJar, appJarFileStatus["length"],
                                                          appJarFileStatus["modificationTime"])
                            },
                            {
                                "key": "__app__.properties",
                                "value": createCacheValue(remoteSparkProperties, sparkPropertiesFileStatus["length"],
                                                          sparkPropertiesFileStatus["modificationTime"])
                            }
                        ]
                    },
                "commands":
                    {
                        "command": "{{JAVA_HOME}}/bin/java -server -Xmx1024m " + \
                                   "-Dhdp.version=2.6.5" + \
                                   "-Dspark.app.name=%s " % appName + \
                                   "org.apache.spark.deploy.yarn.ApplicationMaster " + \
                                   "--class com.zhuyi.DNS.SelectDomain --jar __app__.jar " + \
                                   "--arg 'wannaCry' --arg '2017' --arg '6' --arg '5'  --arg '7'"
                    },
                "environment":
                    {
                        "entry":
                            [
                                {
                                    "key": "JAVA_HOME",
                                    "value": "/usr/java/jdk1.8.0_112/"
                                },
                                {
                                    "key": "SPARK_YARN_MODE",
                                    "value": True
                                },
                                {
                                    "key": "HDP_VERSION",
                                    "value": "2.6.5"
                                },
                                {
                                    "key": "CLASSPATH",
                                    "value": "{{PWD}}<CPS>__spark__.jar<CPS>" + \
                                             "{{PWD}}/__app__.jar<CPS>" + \
                                             "{{PWD}}/__app__.properties<CPS>" + \
                                             "{{HADOOP_CONF_DIR}}<CPS>" + \
                                             "/usr/hdp/current/hadoop-client/*<CPS>" + \
                                             "/usr/hdp/current/hadoop-client/lib/*<CPS>" + \
                                             "/usr/hdp/current/hadoop-hdfs-client/*<CPS>" + \
                                             "/usr/hdp/current/hadoop-hdfs-client/lib/*<CPS>" + \
                                             "/usr/hdp/current/hadoop-yarn-client/*<CPS>" + \
                                             "/usr/hdp/current/hadoop-yarn-client/lib/*<CPS>" + \
                                             "{{PWD}}/mr-framework/hadoop/share/hadoop/common/*<CPS>" + \
                                             "{{PWD}}/mr-framework/hadoop/share/hadoop/common/lib/*<CPS>" + \
                                             "{{PWD}}/mr-framework/hadoop/share/hadoop/yarn/*<CPS>" + \
                                             "{{PWD}}/mr-framework/hadoop/share/hadoop/yarn/lib/*<CPS>" + \
                                             "{{PWD}}/mr-framework/hadoop/share/hadoop/hdfs/*<CPS>" + \
                                             "{{PWD}}/mr-framework/hadoop/share/hadoop/hdfs/lib/*<CPS>" + \
                                             "{{PWD}}/mr-framework/hadoop/share/hadoop/tools/lib/*<CPS>" + \
                                             # "%s<CPS>" % lzoJar[hdpVersion] + \
                                             "/etc/hadoop/conf/secure<CPS>"
                                },
                                {"key":
                                     "SPARK_YARN_CACHE_FILES",
                                 "value": "%s#__app__.jar,%s#__spark__.jar" % (
                                     createHdfsPath(remoteAppJar), createHdfsPath(remoteSparkJar))
                                 },
                                {"key":
                                     "SPARK_YARN_CACHE_FILES_FILE_SIZES",
                                 "value": "%d,%d" % (appJarFileStatus["length"], sparkJarFileStatus["length"])
                                 },
                                {"key":
                                     "SPARK_YARN_CACHE_FILES_TIME_STAMPS",
                                 "value": "%d,%d" % (
                                     appJarFileStatus["modificationTime"], sparkJarFileStatus["modificationTime"])
                                 },
                                {"key":
                                     "SPARK_YARN_CACHE_FILES_VISIBILITIES",
                                 "value": "PUBLIC,PRIVATE"
                                 },
                            ]
                    }
            },
        "unmanaged-AM": False,
        "max-app-attempts": 2,
        "resource": {
            "memory": 67584,
            "vCores": 9
        },
        "application-type": "SPARK",
        "keep-containers-across-application-attempts": False
    }
    sparkJobJson = json.dumps(sparkJob, indent=2, sort_keys=True)
    with open("spark-yarn.json", "w") as fd:
        fd.write(sparkJobJson)
    submitSparkJob(sparkJobJson)
    return newApp[1]["application-id"]


start_program("/zhuyi/submit_files",
              "/zhuyi/submit_filesspark-assembly-1.6.1-hadoop2.6.0.jar",
              "/zhuyi/submit_files/SelectDomain.jar",
              "/zhuyi/submit_files/spark-yarn.properties",
              "submit_files")