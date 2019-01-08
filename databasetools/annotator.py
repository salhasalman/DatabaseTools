from systemtools.basics import *
from datatools.dataencryptor import *
try:
	from systemtools.hayj import *
except: pass
from datastructuretools.hashmap import *
from tkinter import Tk
from databasetools.tkintertest import *

class Annotator:
	def __init__\
	(
		self,
		identifier,
		generator,
		labels,
		taskDescription=None,
		useMongodb=True,
		logger=None,
		verbose=True,
		host=None,
		user=None,
		password=None,
		mongoDbName=None,
	):
		"""
			The generator must throw dict looking:
			{id: "a", "text": "Something to print...",
			"firstLabel": int, "secondLabel": bool}

			If id is None, hash(<text>) will be used.

			text can be a list

		"""
		self.taskDescription = taskDescription
		self.logger = logger
		self.verbose = verbose
		self.identifier = identifier
		self.generator = self.threadedGenerator = threadGen(generator, maxsize=100,
			logger=self.logger, verbose=self.verbose)
		self.labels = labels
		self.useMongodb = useMongodb
		self.annotatorUI = None
		self.host = host
		self.user = user
		self.password = password
		self.mongoDbName = mongoDbName
		self.validityFuncts = dict()
		self.ids = []
		self.index = None
		if self.useMongodb and self.host is None and self.user is None and self.password is None and self.mongoDbName is None:
			try:
				(self.user, self.password, self.host) = getAnnotatorMongoAuth(logger=self.logger)
				self.mongoDbName = "annotator"
			except Exception as e:
				logException(e, self)
				logError("A local storage of labels will be used.", self)
				self.useMongodb
		self.data = SerializableDict\
		(
			name=self.identifier,
	        useMongodb=self.useMongodb,
	        logger=self.logger,
	        verbose=self.verbose,
	        serializeEachNAction=3,
	        host=self.host, user=self.user, password=self.password,
	        useLocalhostIfRemoteUnreachable=False,
	        mongoDbName=self.mongoDbName,
        	mongoIndex="id",
		)

	def startUI(self):
		root = Tk()
		root.geometry("1200x800+600+100")
		self.annotatorUI = AnnotatorUI(rightCallback=self.next, logger=self.logger, verbose=self.verbose, taskDescription=self.taskDescription)
		root.mainloop()

	def addValidityFunctDEPRECATED(self, label, funct):
		"""
			You can give validity functions for each labels:
			annotator.addValidityFunct("firstLabel", lambda x: None if x < 0.0 or x > 1.0 else "The value must be between 0.0 and 1.0")
			These function have to return None or an error message.
		"""
		self.validityFuncts[label] = funct

	def start(self):
		self.startUI()

	def reset(self):
		self.data.clean()
	def clean(self):
		self.reset()



	def previous(self):
		self.update(-1)
	def next(self):
		self.update(+1)
	def update(self, inc):
		# If there is no element already displayed:
		if self.index is not None:
			# We get all labels:

			# We update it:
			pass
		# Finally we load the new content:
		newIndex = self.index + inc
		if newIndex > 0:
			# We update the UI with a new value from the generator:
			if self.index is None or self.index == len(self.ids) - 1:
				try:
					newElement = next(self.generator)
					# ....
				except StopIteration as e:
					log("End of content", self)
					pass # TODO display a message in a status bar ???
				except Exception as e:
					logException(e, self)
			# Else we reload the already displayed element:
			else:
				theNewId = self.ids[newIndex]
				newContent = self.data
			print("NEXT")
			current = next(self.generator)
			self.annotatorUI.initLabelFrame(self.labels)
			self.annotatorUI.initTextFrame(current)

		self.index = newIndex + inc

	def convertElementForUI(self, id):
		if self.data.has(id):
		content = element["content"]
		if 
			data = self.data[id]
			content = data["content"]
		else:
			labels = self.labels
		return (id, content, labels)

	# TODO tester sur mongo student
	# TODO tester sur fichier
	# tester les cleans

def dataGenerator():
	for current in \
	[
		{
			"id": "a",
			"content": \
			[
				{"text": "aaa " * 10000},
				{"title": "Système B", "text": "a " * 1000},
			],
		},
		{
			"id": "b",
			"content": \
			[
				{"text": "bbb " * 10000},
				{"title": "Système B", "text": "b " * 1000},
			],
		},
		{
			"id": "c",
			"content": \
			[
				{"text": "ccc " * 10000},
				{"title": "Système B", "text": "c " * 1000},
			],
		},
	]:
		yield current


if __name__ == "__main__":

	labels = \
	{
		"a": {"title": "test1", "type": LABEL_TYPE.scale, "from": 0.2},
		"b": {"title": "test2", "type": LABEL_TYPE.scale, "default": 0.2},
		"c": {"title": "test3", "type": LABEL_TYPE.scale},
		"d": {"title": "test4", "type": LABEL_TYPE.scale},
		"e": {"title": "They are the same source", "type": LABEL_TYPE.checkbutton},
		"f": {"title": "They are the samethe samethe same source same source", "text": "Same", "type": LABEL_TYPE.checkbutton, "default": True},
	}

	print(bool)
	an = Annotator("test", dataGenerator(), labels, taskDescription="aaaa")
	an.reset()
	an.start()