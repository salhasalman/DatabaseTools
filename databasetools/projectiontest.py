# pew in databasetools-venv python /home/hayj/wm-dist-tmp/DatabaseTools/databasetools/projectiontest.py

import sys, os; sys.path.append("/".join(os.path.abspath(__file__).split("/")[0:-2]))

import pymongo
import random
import string
import time

from systemtools.system import *
from systemtools.duration import *

# We init db name, host...:
dbTest = "student"
host = "localhost"
port = "27017"

# We init users:
if isHostname("hjlat"):
	user = None
	password = None
elif isHostname("datascience01"):
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
def createFakeCollection(alwaysRecreate=True):
	collectionName = "usercrawl"
	mydb = myclient[dbTest]
	mycol = mydb[collectionName]
	if alwaysRecreate or (mycol.count({}) < 100 and (collectionName == "test" or collectionName == "student")):
		mycol.create_index("user_id")
		mycol.delete_many({})
		for i in range(1000):
			text = ""
			for u in range(1000):
				text += ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
			o = {"timestamp": time.time(), "text": text, "user_id": i + 5000}
			mycol.insert_one(o)
	return mycol

def getUserCrawl():
	if not isHostname("datascience01"):
		print("Pls execute on datascience01.") ; exit()
	mydb = myclient["twitter"]
	return mydb["usercrawl"]


def copyUserCrawlCollection(copyCount=1000, alwaysRecreate=False):
	mydb = myclient["twitter"]
	userCrawl = mydb["usercrawl"]
	mydbTest = myclient[dbTest]
	testCol = mydbTest[dbTest]
	testCol.create_index("user_id")
	if alwaysRecreate or testCol.count() < copyCount:
		testCol.delete_many({})
		i = 0
		for row in userCrawl.find({}):
			del row["_id"]
			testCol.insert_one(row)
			if i >= copyCount:
				break
			if i % int(copyCount / 50) == 0:
				print(str(int(i / copyCount * 100)) + "%")
			i += 1
	return testCol



# for row in mycol.find({}):
# 	if not dictContains(row, "user_id"):
# 		print("FUCK")
# 		print(row["url"])
# 		exit()

tt = TicToc()
tt.tic()
mycol = createFakeCollection(alwaysRecreate=False)
# mycol = createFakeCollection(alwaysRecreate=True)
# mycol = getUserCrawl()
# mycol = copyUserCrawlCollection()
tt.tic("collection init DONE")

iterationMaxCount = 20
print("Test started.")
i = 0
for row in mycol.find({}):
	if i % int(iterationMaxCount / 3) == 0:
		print(str(i) + ": " + str(lts(row)[0:60]))
	if i > iterationMaxCount:
		break
	i += 1
tt.tic("Find done.")

iterationMaxCount = 200
i = 0
for row in mycol.distinct("user_id"):
	if i % int(iterationMaxCount / 3) == 0:
		print(str(i) + ": " + str(row)[0:60])
	if i > 200:
		break
	i += 1
tt.tic("Find distincts done.")

i = 0
for row in mycol.find({}, projection={"user_id": True}):
	if i % int(iterationMaxCount / 3) == 0:
		print(str(i) + ": " + str(lts(row)[0:60]))
	if i > 200:
		break
	i += 1
tt.tic("Find with projection done.")

