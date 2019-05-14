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
from multiprocessing import Lock, Process
import copy
from bson.objectid import ObjectId

# TODO expireAfterSeconds: <int> Used to create an expiring (TTL) collection. MongoDB will automatically delete documents from this collection after <int> seconds. The indexed field must be a UTC datetime or the data will not expire.
# unique: if True creates a uniqueness constraint on the index.
#  my_collection.create_index([("mike", pymongo.DESCENDING), # on peut mettre HASHED
#                             ("eliot", pymongo.ASCENDING)]) # Par default ascending

def idsToMongoHost(host="localhost", user=None, password=None, port="27017", databaseRoot=None):
    if host is None:
        host = "localhost"
    if databaseRoot is None:
        databaseRoot = ""
    if user is None:
        user = ""
    if password is None:
        password = ""
    if user is None or user == "":
        return "mongodb://" + host + ":" + port + "/" + databaseRoot
    else:
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
        hideIndexException=False,
    ):
        # All vars :
        self.hideIndexException = hideIndexException
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

        self.scheme = None
        if self.host is not None:
            self.scheme = idsToMongoHost(host=self.host,
                                       user=self.user,
                                       password=self.password,
                                       port=self.port,
                                       databaseRoot=self.databaseRoot)
        self.indexExceptionAlreadyPrinted = False

        # And init the db :
        self.initDataBase()

    def createCompoundIndex(self, indexes, unique=True, background=True):
        if indexes is None or len(indexes) < 1:
            logError("Please set more than 1 index for a compound index!", self)
        else:
            try:
                name = ""
                for current in indexes:
                    if isinstance(current, tuple):
                        current = current[0]
                    name += current + "_"
                name = name[:-1]
                self.collection.create_index(indexes,
                                             name=name,
                                             unique=unique,
                                             background=background)
            except Exception as e:
                if not self.indexExceptionAlreadyPrinted and not self.hideIndexException:
                    logError("Unable to create index " + str(index) + " in " + self.dbName + " " + self.collectionName, self)
                    self.indexExceptionAlreadyPrinted = True

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
                try:
                    self.collection.create_index(index,
                                                 unique=unique,
                                                 background=background)
                except Exception as e:
                    if not self.indexExceptionAlreadyPrinted and not self.hideIndexException:
                        logError("Unable to create index " + str(index) + " in " + self.dbName + " " + self.collectionName, self)
                        self.indexExceptionAlreadyPrinted = True

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

    # def dropAllIndexes(self):
    #     """
    #         drop all indexes of the collection
    #     """
    #     self.dropIndex(self.getIndexes())

    def update(self, query, updateQuery):
#         self.collection.update(query, updateQuery, multi=True)
        self.collection.update_many(query, updateQuery)

    def __setitem__(self, firstIndexOnValue, value):
        key = self.getKeyColumn()
        query = {key: firstIndexOnValue}
        if self.has(firstIndexOnValue):
            self.updateSet(query, value)
        else:
            self.insert(mergeDicts(query, value))
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

    def dropAllIndexes(self):
        for key, value in self.collection.index_information().items():
            if "_id" not in key:
                self.collection.drop_index(key)
    
    def getIndexes(self):
        """
            Return the list of indexes names
        """
        return getIndexes(self.collection)

    # def distinctIds(self):
    #     return self.distinct("_id")
        # try:
        #     return self.distinct("_id")
        # except Exception as e:
        #     log(str(e) + "\nWe retry using batch samples...", self)
        #     return mongoDistinctIds(self.collection,
        #         logger=self.logger, verbose=self.verbose)


    def distinct(self, field="_id", **kwargs):
        try:
            return self.collection.distinct(field, **kwargs)
        except Exception as e:
            if field == "_id":
                log(str(e) + "\nWe retry using batch samples...", self)
                return mongoDistinctIds(self.collection,
                    logger=self.logger, verbose=self.verbose)
            else:
                raise e
                return None
        

    def clone(self):
        return MongoCollection \
        (
            self.dbName,
            self.collectionName,
            version=self.version,
            indexOn=self.indexOn,
            indexNotUniqueOn=self.indexNotUniqueOn,
            giveTimestamp=self.giveTimestamp,
            giveHostname=self.giveHostname,
            verbose=self.verbose,
            logger=self.logger,
            user=self.user,
            password=self.password,
            port=self.port,
            host=self.host,
            databaseRoot=self.databaseRoot,
        )

    def getCloneArgs(self):
        return \
        (
            [
                self.dbName,
                self.collectionName,
            ],
            {
                "version": self.version,
                "indexOn": self.indexOn,
                "indexNotUniqueOn": self.indexNotUniqueOn,
                "giveTimestamp": self.giveTimestamp,
                "giveHostname": self.giveHostname,
                "verbose": self.verbose,
                "logger": self.logger,
                "user": self.user,
                "password": self.password,
                "port": self.port,
                "host": self.host,
                "databaseRoot": self.databaseRoot,
            },
        )

    def initDataBase(self):
        try:
            self.client.close()
        except: pass
        self.client = MongoClient(host=self.scheme)
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
    def size(self, estimate=True):
        """
            Work the same as pymongo collection.count({})
        """
        if estimate:
            return self.collection.estimated_document_count()
        else:
            return self.collection.count_documents({})

    def insert(self, row):
        """
            Insert the given data row, also add version, timestamp and hostname if enabled in the __init__
            And if it not already exists
        """
        if not isinstance(row, list) and not isinstance(row, dict):
            logError("row have to be list or dict", self)
            return False
        else:
            if not isinstance(row, list):
                row = [row]
            for currentRow in row:
                currentRow = copy.deepcopy(currentRow)
                currentRow = dictToMongoStorable(currentRow)
                if self.version is not None and "version" not in currentRow:
                    currentRow["version"] = self.version
                if self.giveTimestamp and "timestamp" not in currentRow:
                    timestamp = time.time()
                    currentRow["timestamp"] = timestamp
                if self.giveHostname and "hostname" not in currentRow:
                    hostname = getHostname()
                    currentRow["hostname"] = hostname
                try:
                    self.collection.insert_one(currentRow)
                    return True
                except Exception as e:
                    logException(e, self)
                    try:
                        log("The row is:\n", self)
                        log("TODO looking for the 'OverflowError: MongoDB can only handle up to 8-byte ints'\n", self)
                        log(lts(reduceDictStr(currentRow)), self)
                    except:
                        pass
                    return False
        return False

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

    def find(self, query={}, limit=0, sort=None, projection=None, **kwargs):
        """
            Works the same as pymongo collection.find but query is optionnal
            sort example: sort=("timestamp", pymongo.DESCENDING)
            Warning, sometimes projection doesn't work for a huge find.
            projection example: projection={‘_id’: True}
        """
        if sort is not None and not isinstance(sort, list):
            sort = [sort]
#         if projection is not None and limit == 0:
#             limit = self.size() + 1
        for i in range(2):
            try:
                return self.collection.find(query, limit=limit, sort=sort, projection=projection, **kwargs)
            except Exception as e:
                logException(e, self)
                time.sleep(0.2)


    def __getitem__(self, o):
        key = self.getKeyColumn()
        return self.findOne({key: o})
    def findOne(self, query={}, projection=None):
        """
            Works the same as pymongo collection.find_one
        """
        for i in range(2):
            try:
                return self.collection.find_one(query, projection=projection) # return None if no element was found
            except Exception as e:
                logException(e, self)
                time.sleep(0.2)

    def removeOne(self, *args, **kwargs):
        return self.deleteOne(*args, **kwargs)
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

    def __delitem__(self, key):
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
        for row in self.find(projection={colName: 1}):
            yield row[colName]

    def items(self, projection=None):
        """
            Return an iteration (key, value) on the first "indexOn"
        """
        colName = self.getKeyColumn()
        if projection is not None:
            projection[colName] = 1
        for row in self.find(projection=projection):
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


    def __contains__(self, key):
        return self.has(key)
    def has(self, query={}):
        """
            This method return True if the given object is found with the first "indexOn"
        """
        # First if the user give a value, we can suppose the key is the first indexOn:
        if not isinstance(query, dict):
            if self.indexOn is not None and len(self.indexOn) > 0:
                query = {self.indexOn[0]: query}
            else:
                theKey = None
                for indexKey in self.getIndexes():
                    if indexKey != "_id":
                        theKey = indexKey
                        break
                if theKey is None:
                    raise Exception("No index to watch...")
                query = {theKey: query}
        # Now we try to find something:
        # return self.findOne(query) is not None
        return self.find(query).count() > 0

    def __len__(self):
        return self.size()

    def map(self, processFunct, lockedProcessInit=None, terminatedFunct=None, parallelProcesses=cpuCount(), limit=None, shuffle=False):
        """
            Callbacks:
             * processFunct: processFunct(row, localCollection, initVars=None). localCollection is a clone of the collection for the current process. Row is the current row to process. initVars is vars you returned in lockedProcessInit.
             * lockedProcessInit: lockedProcessInit(localCollection). Have to return vars of the current process. This callback is called once.
             * terminatedFunct: terminatedFunct(localCollection, initVars=None)

            This function will call processFunct(row, collection=None, initVars=None) given each row of the database in a multiprocessing way. You can use lockedProcessInit(collection) to init vars. processFunct will give you the row and a cloned collection which belong
            to the process.
            See databasetools.test.mongo.Test2
        """
        # Init global vars:
        label = "MongoCollection map: "
        lock = Lock()
        # We get all ids and split them:
        ids = list(self.distinct("_id"))
        if limit is not None:
            ids = ids[:limit]
        pbar = ProgressBar(len(ids), logger=self.logger, verbose=self.verbose, printRatio=0.01)
        q = pbar.startQueue()
        if shuffle:
            random.shuffle(ids)
        idsChunks = split(ids, parallelProcesses)
        # We remove empties cunks:
        newIdsChunks = []
        for current in idsChunks:
            if current is not None and len(current) > 0:
                newIdsChunks.append(current)
        idsChunks = newIdsChunks
        # We execute all processes:
        processes = []
        for chunk in idsChunks:
            localCollectionArgs = self.getCloneArgs()
            p = Process(target=sequentialProcessing, args=
            (
                chunk, lock, localCollectionArgs,
                processFunct, lockedProcessInit, terminatedFunct,
                self.verbose, q,
            ))
            processes.append(p)
        for p in processes:
            p.start()
        for p in processes:
            p.join()
        pbar.stopQueue()
        log("MongoCollection map done.", self)







def mongoDistinctIds(collection, splitSize=None, logger=None, verbose=True, batchSize=10000, useSampleCache=True, sampleCacheDayClean=10):
    from datastructuretools.hashmap import SerializableDict

    # request = {'_id': ObjectId('5a8611880cef4324f19e7a72')}
    # print("START")
    # for current in collection.find(request):
    #     print(current["_id"])

    # exit()

    colSize = collection.estimated_document_count()
    if splitSize is None:
        if batchSize >= colSize:
            splitSize = 3
        else:
            splitSize = int(colSize / batchSize)
    log("Getting a sample of " + str(splitSize) + " ids from " + collection.full_name + " (~4 mins if not in cache)...",
        logger=logger, verbose=verbose)
    tt = TicToc(logger=logger, verbose=verbose)
    tt.tic(display=False)
    pipeline = \
    [
        {"$sample": {"size": splitSize}},
        {"$group": {"_id": None, "ids": {"$addToSet": "$_id"}}}
    ]
    idsSample = None
    sampleCache = None
    pipelineHash = objectAsKey(pipeline) + "_" + collection.full_name
    if useSampleCache:
        sampleCache = SerializableDict(name="mongo-ids-sample", useMongodb=False, limit=500, cleanNotReadOrModifiedSinceNDays=sampleCacheDayClean)
        if pipelineHash in sampleCache:
            idsSample = sampleCache[pipelineHash]
            newIdsSample = []
            for current in idsSample:
                newIdsSample.append(ObjectId(current))
            idsSample = newIdsSample
    if idsSample is None:
        aggregation = collection.aggregate(pipeline)
        for current in aggregation:
            idsSample = current["ids"]
        idsSample = sorted(idsSample)
        if useSampleCache:
            sampleCache[pipelineHash] = idsSample
            sampleCache.save()
        tt.toc()
    ids = set()
    pbar = ProgressBar(len(idsSample) + 1, printRatio=0.1,
        logger=logger, verbose=verbose)
    log("Getting all distincts ids from " + collection.full_name + "...",
        logger=logger, verbose=verbose)
    # Sometimes we need to cast ObjectIds to str, so I handle the case
    # doing a misc request:
    castToStr = False
    miscResult = collection.find_one({"_id": idsSample[0]})
    if miscResult is None:
        castToStr = True
    for u in range(-1, len(idsSample)):
        match = {"$match": {"_id": {}}}
        condition = match["$match"]["_id"]
        if u != -1:
            # WHY ? Maybe due to the version... TODO handle this case...
            if castToStr:
                condition["$gte"] = str(idsSample[u]) # v3.2.20
            else:
                condition["$gte"] = idsSample[u] # v3.4.9
        if u + 1 < len(idsSample):
            if castToStr:
                condition["$lt"] = str(idsSample[u + 1])
            else:
                condition["$lt"] = idsSample[u + 1]
        pipeline = \
        [
            match,
            {"$group": {"_id": None, "ids": {"$addToSet": "$_id"}}},
        ]
        aggregation = collection.aggregate(pipeline)
        currentIds = None
        for current in aggregation:
            currentIds = current["ids"]
        # print(currentIds)
        if currentIds is not None:
            for currentId in currentIds:
                ids.add(currentId)
        pbar.tic()
    if len(ids) < colSize:
        logWarning("The estimated size of the collection " + collection.full_name + " (" + str(colSize) + ") is higher than the distinct ids count (" + str(len(ids)) + ").", logger, verbose=verbose)
    return ids


# We define the function which will be called for each chunk:
def sequentialProcessing(chunk, lock, localCollectionArgs, processFunct, lockedProcessInit, terminatedFunct, verbose, q, progressionPercentDisp=1):
    if chunk is None or len(chunk) == 0:
        return
    # We make all local vars:
    label = "MongoCollection map: "
    name = getRandomName()
    localTT = TicToc()
    localTT.tic(display=False)
    initVars = None
    # We init all proceses:
    with lock:
        localCollection = MongoCollection(*localCollectionArgs[0], **localCollectionArgs[1])
        if lockedProcessInit is not None:
            initVars = lockedProcessInit(localCollection)
        duration = truncateFloat(localTT.toc(display=False), 2)
        log(label + name + " initialized in " + str(duration) + "s.", verbose=verbose)
    # We make all counters:
    i = 0
    # We iterate on all ids:
    for id in chunk:
        row = localCollection.findOne({"_id": id})
        try:
            processFunct(row, localCollection, initVars=initVars)
        except Exception as e:
            logException(e, logger, message=label + name,
                location="MongoCollection.map")
        i += 1
        q.put(None) # For the ProgressBar
    if terminatedFunct is not None:
        terminatedFunct(localCollection, initVars=initVars)

def toMongoStorable(*args, **kwargs):
    return dictToMongoStorable(*args, **kwargs)

def dictToMongoStorable(data, logger=None, verbose=True, dollarEscape="__mongostorabledollar__", normalizeKeys=True, normalizeEnums=True, normalizeBigInts=True, convertTuples=True, convertSets=True):
    """
        This function convert all set() in list()
        and all "." in keys will be replaced by "_"
    """
    kwargs = \
    {
        "dollarEscape": dollarEscape,
        "logger": logger,
        "verbose": verbose,
        "normalizeKeys": normalizeKeys,
        "normalizeEnums": normalizeEnums,
        "normalizeBigInts": normalizeBigInts,
        "convertTuples": convertTuples,
        "convertSets": convertSets,
    }
    if data is None:
        return None
    if isinstance(data, tuple):
        if convertTuples:
            data = list(data)
        else:
            return data
    if isinstance(data, int):
        if normalizeBigInts and intByteSize(data) >= 8:
            return str(data)
        else:
            return data
    if isinstance(data, str) or \
       isinstance(data, float) or \
       isinstance(data, bool):
        return data
    if isinstance(data, enum.Enum) and hasattr(data, 'name'):
        if normalizeEnums:
            return data.name
        else:
            return data
    if isinstance(data, set):
        # logWarning("Can't store a set in MongoDB!", logger=logger, verbose=verbose)
        if convertSets:
            return list(data)
        else:
            return data
    if isinstance(data, list):
        newList = []
        for current in data:
            newList.append(dictToMongoStorable(current, **kwargs))
        return newList
    if isinstance(data, dict):
        newData = {}
        for key, value in data.items():
            value = dictToMongoStorable(value, **kwargs)
            if normalizeKeys:
                key = key.replace(".", "_")
                if key.startswith("$"):
                    key = dollarEscape + key[1:]
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









