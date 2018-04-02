

# MongoCollection

This class allow an easy config of a MongoDB collection by providing an interface which handle authentication, indexes management, data conversion and pretty print of the collection. It can work like a Python dict if you give at least one index.

## Install

	git clone https://github.com/hayj/DomainDuplicate.git
	pip install ./DomainDuplicate/wm-dist/*.tar.gz

## Usage

	>>> from databasetools.mongo import *
	>>> collection = MongoCollection("db_name", "collection_name", indexOn=["user_id"],
				indexNotUniqueOn=["job"])

## Features

 * Easy config of the MongoDB auth by giving `user`, `password` and `host` in the class init
 * Easy config of indexes (unique or not) by giving a list of index names in the class init
 * Can "show" the collection or the database in a pretty print using methods `show` and `showDbs`
 * Automatically convert your data which is not "mongo storable" (sets, enums, objects...)
 * Automatically provide a "version" (if given), a timestamp, and optionally the "hostname" of your computer for each inserted rows
 * The first `indexOn` works like the key of a Python dict so you can use `in` (e.g. `15466 in collection` which is equivalent to `pymongoCollection.find_one({"user_id": 15466}) is not None`).
 * You can also use `__len__` (e.g. `len(collection)`), `__getitem__` (e.g. `collection[15466]`, only in the case you gave at least one `indexOn` at the `__init__`), `__setitem__` and `__delitem__`.
 * Provide top levels methods like `toDataFrame`, `renameField`, `createIndex`, `getIndexes`, `dropAllIndexes`, `dropIndex`, `resetDatabase`, `resetCollection`... See the code documentation for more details.

## Parameters

 * **dbName**: The name of the database, will be created if not exists (mandatory)
 * **collectionName**: The name of the collection, will be created if not exists (mandatory)
 * **host**, **user** and **password**: For the Mongo database (default is "localhost" with no auth)
 * **port**: The port (default "27017")
 * **indexOn**: The name of the index, or a list of indexes names, the first one enable the MongoCollection instance working like a Python dict with `__getitem__`, `__setitem__`, `__contains__`...
 * **indexNotUniqueOn**: A name for a "not unique index" or a list of names
 * **giveTimestamp**: Set it as `True` if you want to add a timestamp on each inserted documents
 * **giveHostname**: Set it as `True` if you want to add the hostname of your computer in documents
 * **version**: The version of row you will insert as a new column
 * **databaseRoot**: In the case your user has a main database which is not admin, you can set it via this init param
 * **logger**: A logger from `systemtools.logger` (see [SystemTools](https://github.com/hayj/SystemTools))
 * **verbose**: To set the verbose (`True` or `False`)

## MongoDB installation

If you are on Ubuntu, I recommend to follow this tutorial if you want to install MongoDB on localhost : <https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/>. Or you can open the database and secure it: <https://www.digitalocean.com/community/tutorials/how-to-install-and-secure-mongodb-on-ubuntu-16-04>.
