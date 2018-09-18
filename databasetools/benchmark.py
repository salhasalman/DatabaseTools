# pew in databasetools-venv python /home/hayj/wm-dist-tmp/DatabaseTools/databasetools/benchmark.py

import sys, os; sys.path.append("/".join(os.path.abspath(__file__).split("/")[0:-2]))

from enum import Enum
import pymongo
import random
import string
import time

from systemtools.system import *
from systemtools.duration import *
from datastructuretools.processing import *

# We init db name, host...:
dbTest = "student"
collectionName = "fake"
port = "27017"
rowCount = 10000
randomElementCount = 100000
# if isHostname("hjlat"):
#     rowCount = rowCount / 100
#     randomElementCount = randomElementCount / 100

# We init users:
if isHostname("hjlat"):
    host = "212.129.44.40"
    user = "student"
    password = "cs-lri-octopeek-984135584714323587326"
elif isHostname("datascience01"):
    host = "localhost"
    user = "student"
    password = "cs-lri-octopeek-984135584714323587326"
else:
    print("Pls execute on hjlat or datascience01.") ; exit()

# We make the mongo scheme:
mongoConnectionScheme = "mongodb://" + host + ":" + port
if user is not None:
    mongoConnectionScheme = "mongodb://" + user + ":" + password + "@" + host + ":" + port
myclient = pymongo.MongoClient(mongoConnectionScheme)

# The function which create a fake collection:
def createFakeCollection(alwaysRecreate=True, deletePreviousRows=False):
    mydb = myclient[dbTest]
    mycol = mydb[collectionName]
    colCount = mycol.count({})
    if alwaysRecreate or colCount < int(rowCount / 4):
        mycol.create_index("user_id")
        if deletePreviousRows:
            mycol.delete_many({})
        for i in range(rowCount - colCount):
            if i % int(rowCount / 20) == 0:
                print("createFakeCollection: " + str(int(i / rowCount * 100)) + "%")
            text = ""
            for u in range(randomElementCount):
                text += ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            o = {"timestamp": time.time(), "text": text}
            mycol.insert_one(o)
    return mycol

# The function wich parse a row:
def parse(row):
    theHash = md5(row["text"])
    for i in range(config["hashingIterationCount"]):
        theHash = md5(theHash)
    return theHash

# Init of the collection:
tt = TicToc()
tt.tic()
mycol = createFakeCollection(alwaysRecreate=False)
tt.tic("collection init DONE")

# Benchmark parameters to vary:
EXEC_MODE = Enum("RESULT_MODE", "multithreading multiprocessing sequential")
RESULT_MODE = Enum("EXEC_MODE", "insert update localadd")
config = \
{
    "hashingIterationCount": 10000,
    "rowToParseAmount": 10000, # 10000
    "parallelParsing": 8, # 16
    "execMode": EXEC_MODE.multiprocessing,
    "resultMode": RESULT_MODE.localadd,
}
context = \
{
    "host": getHostname(),
    "colsize": mycol.count({}),
}
# TODO faire 2 modes, un mode update de la même lligne,
# un mode insert dans une autre base de données,
# et un mode qui met dans une liste
# Dans tous les cas afficher un count à la fin
# TODO tester mutlithreading vs multi processing
# TODO tester sur datascience01 et hjlat

# We define a function which iterate, parse all lines
# and finally gather all result in a list
def sequentialGathering(ids, verbose=True):
    currentClient = pymongo.MongoClient(mongoConnectionScheme)
    currentDB = currentClient[dbTest]
    currentCol = currentDB[collectionName]
    data = []
    i = 0
    for id in ids:
        row = currentCol.find_one({"_id": id})
        if i % int(config["rowToParseAmount"] / 20) == 0:
            if verbose:
                print("sequentialGathering: " + str(int(i / config["rowToParseAmount"] * 100)) + "%")
        if i >= config["rowToParseAmount"]:
            break
        theHash = parse(row)
        data.append(theHash)
        i += 1
    return data


# We first get all ids:
ids = list(mycol.distinct("_id"))
tt.tic("Getting all ids DONE")

# We benchmark sequentialGathering:
if config["execMode"] == EXEC_MODE.sequential:
    data = sequentialGathering(ids)
    print(lts(data)[:60])
    print(len(data))
else:
    ids = split(ids, config["parallelParsing"])
    mapType = None
    if config["execMode"] == EXEC_MODE.multithreading:
        mapType = MAP_TYPE.multithreadMap
    elif config["execMode"] == EXEC_MODE.multiprocessing:
        mapType = MAP_TYPE.multiprocessing
    pool = Pool(parallelCount=config["parallelParsing"],
                mapType=mapType)
    result = pool.map(ids, sequentialGathering)
    data = []
    for current in result:
        data = data + current
    print(lts(data)[:60])
    print(len(data))


tt.tic("exec done.")


print("config:\n" + lts(config))
print("context:\n" + lts(context))

