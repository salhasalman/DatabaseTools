# import pymongo

user = "student"
password = "cs-lri-octopeek-984135584714323587326"
host = "212.129.44.40"
# host = "localhost"
port = "27017"
collectionName = "domainduplicate"
dbName = "serializabledict"




# mongoConnectionScheme = "mongodb://" + user + ":" + password + "@" + host + ":" + port

# myclient = pymongo.MongoClient(mongoConnectionScheme)


# db = myclient[dbName]
# col = db[collectionName]


# print(col.count({}))



from databasetools.mongo import *
col = MongoCollection(dbName, collectionName, user=user, password=password, host=host)

print(col.size())