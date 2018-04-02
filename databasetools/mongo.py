# coding: utf-8

from pymongo import MongoClient
import pandas as pd
import time
from systemtools.logger import *
from systemtools.basics import *
from systemtools.logger import *
from systemtools.system import *
from systemtools.hayj import *
from datastructuretools.processing import *
import urllib
import enum

# TODO expireAfterSeconds: <int> Used to create an expiring (TTL) collection. MongoDB will automatically delete documents from this collection after <int> seconds. The indexed field must be a UTC datetime or the data will not expire.
# unique: if True creates a uniqueness constraint on the index.
#  my_collection.create_index([("mike", pymongo.DESCENDING), # on peut mettre HASHED
#                             ("eliot", pymongo.ASCENDING)]) # Par default ascending

def idsToMongoHost(host="localhost", user=None, password=None, port="27017", databaseRoot=None):
    if host is None or user is None:
        return None
    if databaseRoot is None:
        databaseRoot = ""
    if user is None:
        user = ""
    if password is None:
        password = ""
    return "mongodb://" + user + ":" + password + "@" + host + ":" + port + "/" + databaseRoot

def getIndexes(collection):
    return list(getIndexesGenerator(collection))
def getIndexesGenerator(collection):
    for name, theDict in collection.index_information().items():
        for key, count in theDict["key"]:
            yield key

def getDatabaseNames(client):
    return list(client.database_names())

def getCollectionNames(database):
    return list(database.collection_names(include_system_collections=False))

def databaseToDictShow(client, limit=1):
    result = {}
    databaseNames = getDatabaseNames(client)
    for databaseName in databaseNames:
        if databaseName != "local" and databaseName != "admin": #  and databaseName != "test"
            database = client[databaseName]
            result[databaseName] = {}
            collectionNames = getCollectionNames(database)
            for collectionName in collectionNames:
                collection = database[collectionName]
                result[databaseName][collectionName] = collectionToDictShow(collection, limit=limit, onlyFirstLevel=True)
    return result

def collectionToDictShow(collection, limit=10, sort=None, onlyFirstLevel=False):
    if sort is not None and not isinstance(sort, list):
        sort = [sort]

    count = collection.count({})
    indexes = str(getIndexes(collection))
    result = {"count": count, "indexes": indexes, "data": []}
    for current in collection.find({}, limit=limit, sort=sort):
        if onlyFirstLevel:
            newCurrent = {}
            for key, value in current.items():
                newCurrent[key] = str(value)
            current = newCurrent
        current = reduceDictStr(current, replaceNewLine=True, reduceLists=True)
        result["data"].append(current)
    return result

class MongoCollection():
    def __init__ \
    (
        self,
        dbName,
        collectionName,
        version=None,
        indexOn=None,
        indexNotUniqueOn=None,
        giveTimestamp=True,
        giveHostname=False,
        verbose=True,
        logger=None,
        user=None,
        password=None,
        port="27017",
        host=None,
        databaseRoot=None,
    ):
        """
            Read the README
        """
        # All vars :
        self.logger = logger
        self.verbose = verbose
        self.version = version
        self.port = port
        self.host = host
        self.user = user
        self.password = password
        try:
            if self.password is not None:
                self.password = urllib.parse.quote_plus(self.password)
        except Exception as e:
            logException(e, self)
        self.dbName = dbName
        if indexOn is not None:
            if not isinstance(indexOn, list):
                indexOn = [indexOn]
        if indexNotUniqueOn is not None:
            if not isinstance(indexNotUniqueOn, list):
                indexNotUniqueOn = [indexNotUniqueOn]
        self.indexOn = indexOn
        self.indexNotUniqueOn = indexNotUniqueOn
        self.giveTimestamp = giveTimestamp
        self.giveHostname = giveHostname
        self.collectionName = collectionName
        self.databaseRoot = databaseRoot
        if self.databaseRoot is None:
            self.databaseRoot = ""

        if self.host is not None:
            self.host = idsToMongoHost(host=self.host,
                                       user=self.user,
                                       password=self.password,
                                       port=self.port,
                                       databaseRoot=self.databaseRoot)

        # And init the db :
        self.initDataBase()

    def createIndexes(self, *args, **kwargs):
        self.createIndex(*args, **kwargs)
    def createIndex(self, indexes, unique=True, type=None, background=True):
        """
            Create an index (or a list of) by name(s), unique indicate if the index has to be unique
        """
        if indexes is not None:
            if not isinstance(indexes, list):
                indexes = [indexes]
            for index in indexes:
                if type is not None:
                    index = [(index, type)]
                self.collection.create_index(index,
                                             unique=unique,
                                             background=background)

    def rename(self, newName):
        """
            rename the collection
        """
        if newName is None or not isinstance(newName, str) or len(newName) <= 1 or len(newName) > 30:
            exeptionMessage = "newName is not valid: " + str(newName)
            logError(exeptionMessage, self)
            raise Exception(exeptionMessage)
        self.collection.rename(newName)

    def renameField(self, old, new, force=False):
        """
            Rename the field "old" in all documents by "new"
        """
        logInfo("WARNING : a rename can take a lot of disk space.", self)
        if not force:
            askContinue()
        self.update({}, {'$rename': {old: new}})

    def dropAllIndexes(self):
        """
            drop all indexes of the collection
        """
        self.dropIndex(self.getIndexes())

    def update(self, query, updateQuery):
#         self.collection.update(query, updateQuery, multi=True)
        self.collection.update_many(query, updateQuery)



    def __setitem__(self, firstIndexOnValue, value): # TODO test
        key = self.getKeyColumn()
        query = {key: firstIndexOnValue}
        self.updateSet(query, value)
    def updateSet(self, query, setQuery):
        """
            Update rows but add the "$set" key automatically
        """
        setQuery = {"$set": setQuery}
        self.collection.update_many(query, setQuery)

    def updateOne(self, query, updateQuery):
        """
            Works the same as pymongo collection.update_one
        """
        self.collection.update_one(query, updateQuery)

#     def show(self, *args, **kwargs):
#         for current in self.find(None, *args, **kwargs):
#             log(listToStr(current), self)
#         log("size=" + str(self.size()), self)

    def show(self, limit=10, sort=None, onlyFirstLevel=False):
        """
            Do a pretty print of the collection

            :params:
            limit is the number of element to print
            onlyFirstLevel as True display only first level in document tree
        """
        result = collectionToDictShow(self.collection, limit=limit, sort=sort, onlyFirstLevel=onlyFirstLevel)
        log(listToStr(result), self)

    def showDbs(self, limit=1):
        """
            Do a pretty print of all databases
        """
        log(listToStr(databaseToDictShow(self.client, limit=limit)), self)

    def getInfos(self):
        """
            Works the same as pymongo collection.index_information()
        """
        return self.collection.index_information()

    def dropIndexes(self, *args, **kwargs):
        self.dropIndex(*args, **kwargs)
    def dropIndex(self, indexes):
        """
            Drop the index by name
        """
        if not isinstance(indexes, list):
            indexes = [indexes]
        namesToDelete = []
        for index in indexes:
            if index != "_id":
                infos = self.collection.index_information()
                for name, theDict in infos.items():
                    for key, count in theDict["key"]:
                        if key == index:
                            namesToDelete.append(name)
        for name in namesToDelete:
            self.collection.drop_index(name)



    def getIndexes(self):
        """
            Return the list of indexes names
        """
        return getIndexes(self.collection)

    def initDataBase(self):
        try:
            self.client.close()
        except: pass
        self.client = MongoClient(host=self.host)
        self.db = self.client[self.dbName]
        self.collection = self.db[self.collectionName]
        self.createIndexes(self.indexOn, unique=True)
#         if self.indexOn is not None and len(self.indexOn) > 0:
#             for currentIndexOn in self.indexOn:
#                 self.collection.create_index(currentIndexOn, unique=True)
        self.createIndexes(self.indexNotUniqueOn, unique=False)
#         if self.indexNotUniqueOn is not None and len(self.indexNotUniqueOn) > 0:
#             for currentIndexOn in self.indexNotUniqueOn:
#                 self.collection.create_index(currentIndexOn, unique=False)
        log(self.title(), self)

    def title(self):
        result = self.dbName + " " + self.collectionName
        if self.version is not None:
            result += " (version " + str(self.version) + ")"
        result += " initialised."
        return result


    def removeRowsVersion(self, version):
        pass # TODO

    @staticmethod
    def userAllow(elementName="the data"):
        answer = input('Do you confirm ' + elementName + ' removal ? (y/n)')
        if answer == "y":
            return True
        return False

    def resetCollection(self, security=True):
        if self.size() > 0:
            if not security or MongoCollection.userAllow("the collection " + str(self.collectionName)):
                self.collection.drop()
                self.initDataBase()

    def resetDatabase(self, security=True):
        if not security or MongoCollection.userAllow("the database " + str(self.dbName)):
            self.client.drop_database(self.dbName)
            self.initDataBase()

    def getIndexesSize(self):
        """
            Return the index size
        """
        count = 0
        for _ in self.collection.list_indexes():
            count += 1
        return count

    def toString(self):
        result = ""
        for entry in self.collection.find({}):
            result += self.entryToString(entry) + "\n"
        return result

    def __len__(self):
        return self.size()
    def count(self):
        return self.size()
    def size(self):
        """
            Work the same as pymongo collection.count({})
        """
        return self.collection.count({})

    def insert(self, row):
        """
            Insert the given data row, also add version, timestamp and hostname if enabled in the __init__
            And if it not already exists
        """
        if not isinstance(row, list) and not isinstance(row, dict):
            logError("row have to be list or dict", self)
        else:
            if not isinstance(row, list):
                row = [row]
            for currentRow in row:
                currentRow = dictToMongoStorable(currentRow)
                if self.version is not None and "version" not in currentRow:
                    currentRow["version"] = self.version
                if self.giveTimestamp and "timestamp" not in currentRow:
                    timestamp = time.time()
                    currentRow["timestamp"] = timestamp
                if self.giveHostname and "hostname" not in currentRow:
                    hostname = getHostname()
                    currentRow["hostname"] = hostname
                self.collection.insert_one(currentRow)

#     def parallelUpdate(self, query, updateQuery, parallelUpdates=None):
#         def getAllIds(request, collection):
#             allIdsCursor = collection.collection.find(request, {"_id": True})
#             allIds = []
#             for current in allIdsCursor:
#                 allIds.append(current["_id"])
#             random.shuffle(allIds)
#             return allIds
#         def chunkUpdate(ids):
#             for id in ids:
#                 self.updateOne({"_id": id}, updateQuery)
#         allIds = getAllIds(query, self.collection)
#         if parallelUpdates is None:
#             parallelUpdates = cpuCount()
#         allIds = chunks(allIds, parallelUpdates)
#         pool = MPPool(parallelUpdates)
#         pool.map(chunkUpdate, allIds)
#         if self.collection.has(query):
#             logError("It remains old rows.", self)
#         else:
#             log("All were updated!", self)

    def sample(self, count=100):
        # https://stackoverflow.com/questions/12664816/random-sampling-from-mongo
        pass # TODO

    def find(self, query={}, limit=0, sort=None, projection=None):
        """
            Works the same as pymongo collection.find but query is optionnal
        """
        if sort is not None and not isinstance(sort, list):
            sort = [sort]
        return self.collection.find(query, limit=limit, sort=sort, projection=projection)


    def __getitem__(self, o): # TODO test
        key = self.getKeyColumn()
        return self.findOne({key: o})
    def findOne(self, query={}):
        """
            Works the same as pymongo collection.find_one
        """
        return self.collection.find_one(query) # return None if no element was found

    def deleteOne(self, query=None):
        """
            Works the same as collection.delete_one but doesn't throw any Exception
        """
        if query is not None:
            try:
                return self.collection.delete_one(query)
            except Exception as e:
                logError(str(e), self)
        return None

    def __delitem__(self, key): # TODO test
        """
            Delete the element which amtch with the first "indexOn"
        """
        colName = self.getKeyColumn()
        self.deleteOne({colName: key})

    def getKeyColumn(self):
        """
            Return the first "indexOn" given in the __init__
        """
        colName = "_id"
        if self.indexOn is not None and len(self.indexOn) == 1:
            colName = self.indexOn[0]
        return colName

    def keys(self):
        """
            Return column names
        """
        colName = self.getKeyColumn()
        for row in self.find():
            yield row[colName]

    def items(self):
        """
            Return an iteration (key, value) on the first "indexOn"
        """
        colName = self.getKeyColumn()
        for row in self.find():
            yield (row[colName], row)

    def delete(self, query=None):
        if query is not None:
            try:
                return self.collection.delete_many(query)
            except Exception as e:
                logError(str(e), self)
        return None

    def toDataFrame(self, deleteMongoId=False):
        """
            Convert the collection in a pandas dataframe
        """
        allRows= self.find()
        if deleteMongoId:
            for i in range(len(allRows)):
                allRows[i].pop("_id")
        df = pd.DataFrame(list(allRows))
        return df


    def __contains__(self, key): # TODO test
        return self.has(key)
    def has(self, query={}):
        """
            This method return True if the given object is found with the first "indexOn"
        """
        # First if the user give a value, we can suppose the key is the first indexOn:
        if not isinstance(query, dict):
            query = {self.indexOn[0]: query}
        # Now we try to find something:
        return self.findOne(query) is not None

    def __len__(self):
        return self.size()

def dictToMongoStorable(data, logger=None, verbose=True):
    """
        This function convert all set() in list()
        and all "." in keys will be replaced by "_"
    """
    if data is None:
        return None
    if isinstance(data, tuple):
        data = list(data)
    if isinstance(data, str) or \
       isinstance(data, int) or \
       isinstance(data, float) or \
       isinstance(data, bool):
        return data
    if isinstance(data, enum.Enum) and hasattr(data, 'name'):
        return data.name
    if isinstance(data, set):
        # logWarning("Can't store a set in MongoDB!", logger=logger, verbose=verbose)
        return list(data)
    if isinstance(data, list):
        newList = []
        for current in data:
            newList.append(dictToMongoStorable(current))
        return newList
    if isinstance(data, dict):
        newData = {}
        for key, value in data.items():
            value = dictToMongoStorable(value)
            key = key.replace(".", "_")
            newData[key] = value
        return newData
    if isinstance(data, object):
#         logWarning("Can't store an object in MongoDB!", logger=logger, verbose=verbose)
        return str(data)
    else:
        return data

def testDisplay():
    mc = MongoCollection("test", "collection1", verbose=True, indexOn="t")
    mc.resetCollection(security=False)
    for i in range(1000):
        mc.insert({"t": i})
    print(mc.toDataFrame())
    print("size", mc.size())

def testConvert():
    dict1 = {"a.b": {1, 2, 3, 3}, "b": {"c.n": None, "c_m": {"d.3": 1.0001}}}
    printLTS(dictToMongoStorable(dict1))

if __name__ == '__main__':
    testConvert()









