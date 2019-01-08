# pew in st-venv python ~/Workspace/Python/Utils/DatabaseTools/databasetools/tkintertest.py


from tkinter import Tk, Text, BOTH, CENTER, LEFT, Message, font, PhotoImage, Entry, Scale, HORIZONTAL, Checkbutton, IntVar, INSERT, DISABLED, Scrollbar
from tkinter.ttk import Frame, Label, Style, Button, Separator
from systemtools.logger import *


import enum
# All type has title, type
# scale has: default, resolution, from, to
# option has: options, default
# checkbutton has: default (boolean), shorttitle (facultative, if None, the title will be taken)
LABEL_TYPE = enum.Enum("LABEL_TYPE", "scale option entry, checkbutton")


class AnnotatorUI(Frame):
	def __init__(self, logger=None, verbose=True, toolDescription="This tool allow you to labelize data one by one. Use left/right keys or buttons below to switch to another data or come back to the previous.", taskDescription=None,
		leftCallback=None, rightCallback=None):
		super().__init__()
		self.leftCallback = leftCallback
		self.rightCallback = rightCallback
		self.logger = logger
		self.verbose = verbose
		self.taskDescription = taskDescription
		self.toolDescription = toolDescription
		self.messageWidth = 300
		self.messageFont = "Arial 8 bold italic"
		self.labelMessages = None
		self.labelEntries = None
		self.labelFrame = None
		self.labelFrameRow = None
		self.headMessagesOption = \
		{
			"width": self.messageWidth,
			"font": "Arial 12 bold",
			"bg": "grey",
			"foreground": "white",
		}
		self.textFrame = None
		self.initUI()
		# self.fillWithFakeData()
		self.initKeyEvents()

	def initKeyEvents(self):
		self.bind_all("<Key>", self.keypress)

	def keypress(self, e):
		try:
			if e.keycode == 113:
				self.left()
			elif e.keycode == 114:
				self.right()
		except Exception as e:
			logException(e, self)

	def fillWithFakeData(self):
		labels = \
		[
			{"title": "test1", "type": LABEL_TYPE.scale, "from": 0.2},
			{"title": "test2", "type": LABEL_TYPE.scale, "default": 0.2},
			{"title": "test3", "type": LABEL_TYPE.scale},
			{"title": "test4", "type": LABEL_TYPE.scale},
			{"title": "They are the same source", "type": LABEL_TYPE.checkbutton},
			{"title": "They are the samethe samethe same source same source", "shorttitle": "Same", "type": LABEL_TYPE.checkbutton, "default": True},
		]
		self.initLabelFrame(labels)
		texts = \
		[
			{"text": "aaa bbb CCC " * 1000},
			{"title": "test2", "text": "aaa bbb CCC " * 1000},
		]
		self.initTextFrame(texts)

	def initTextFrame(self, texts):
		# We init the frame:
		if self.textFrame is not None:
			self.textFrame.destroy()
		self.textFrame = Frame(self)
		self.textFrame.grid(row=0, column=0, columnspan=10, rowspan=10, 
			padx=5, pady=0, sticky="news")
		self.textFrame.rowconfigure(0, weight=0)
		self.textFrame.rowconfigure(1, weight=1)
		# For each text:
		nbColumns = len(texts)
		for i in range(nbColumns):
			self.textFrame.columnconfigure(i, weight=1)
			current = texts[i]
			try:
				# We add the head message:
				if dictContains(current, "title"):
					headMessage = Message(self.textFrame, text=current["title"], **self.headMessagesOption)
					headMessage.grid(row=0, column=i, sticky='nwe', padx=2, pady=0)
				# We create the area for the text:
				textAreaFrame = Frame(self.textFrame)
				textAreaFrame.columnconfigure(0, weight=1)
				textAreaFrame.rowconfigure(0, weight=1)
				textAreaRow = 1
				textAreaRowspan = 1
				if not dictContains(current, "title"):
					textAreaRow = 0
					textAreaRowspan = 2
				textAreaFrame.grid(row=textAreaRow, rowspan=textAreaRowspan, column=i, sticky="news", padx=0)
				# We create the Text widget in:
				textWidget = Text(textAreaFrame)
				textWidget.grid(row=0, column=0, sticky="news")
				textWidget.insert(INSERT, current["text"])
				textWidget.config(state=DISABLED)
				# We make the scroll bar:
				scrollBar = Scrollbar(textAreaFrame, command=textWidget.yview)
				scrollBar.grid(row=0, column=1, sticky="nse")
				textWidget['yscrollcommand'] = scrollBar.set
			except Exception as e:
				logException(e, self)

	   
	def initLabelFrame(self, labels):
		# We init the frame:
		if self.labelFrame is not None:
			self.labelFrame.destroy()
		self.labelMessages = []
		self.labelEntries = []
		self.labelFrame = Frame(self)
		self.labelFrame.grid(row=self.labelFrameRow, column=11, sticky="nsew")
		self.labelFrame.columnconfigure(0, weight=1)
		# We init the row counter:
		i = 0
		# We make the head message:
		self.labelFrame.rowconfigure(i, weight=1)
		title = Message(self.labelFrame, text="Labels", **self.headMessagesOption)
		title.grid(row=i, column=0, sticky='nsew', pady=10)
		i += 1
		# For each label:
		for label in labels:
			try:
				# We make the message:
				currentMessage = None
				if label["type"] != LABEL_TYPE.checkbutton or dictContains(label, "shorttitle"):
					currentMessage = Message(self.labelFrame, text=label["title"], width=self.messageWidth, font=self.messageFont) # bg="white", foreground="black"
				# According to the label type:
				if not dictContains(label, "type") or label["type"] == LABEL_TYPE.scale:
					# We make the entry:
					currentEntry = Scale\
					(
						self.labelFrame,
						from_=label["from"] if dictContains(label, "from") else 0.0,
						to=label["to"] if dictContains(label, "to") else 1.0,
						orient=HORIZONTAL,
						resolution=label["resolution"] if dictContains(label, "resolution") else 0.05,
					)
					currentEntry.set(label["default"] if dictContains(label, "default") else 0.5)
				elif label["type"] == LABEL_TYPE.checkbutton:
					currentEntry = Checkbutton\
					(
						self.labelFrame,
						text=label["shorttitle"] if dictContains(label, "shorttitle") else label["title"],
					)
					if dictContains(label, "default") and label["default"]:
						currentEntry.select()
				# We grid the message:
				if currentMessage is not None:
					self.labelFrame.rowconfigure(i, weight=1)
					currentMessage.grid(row=i, column=0, sticky="nsew")
					i += 1
				# We grid the entry:
				self.labelFrame.rowconfigure(i, weight=1)
				currentEntry.grid(row=i, column=0, sticky="nsew")
				i += 1
				# We make a separator:
				self.labelFrame.rowconfigure(i, weight=1)
				sep = Separator(self.labelFrame, orient=HORIZONTAL)
				sep.grid(row=i, column=0, sticky='nsew', pady=10)
				i += 1
			except Exception as e:
				logException(e, self)
		
	def initUI(self):
		self.master.title("Annotator")
		# self.grid(row=0, column=0, sticky='news')
		# self.rowconfigure(0, weight=1)
		# self.columnconfigure(0, weight=1)
		self.pack(fill=BOTH, expand=True)
		self.columnconfigure(0, weight=1)
		self.columnconfigure(11, pad=10)
		for i in range(10):
			self.rowconfigure(i, weight=1)
		# for i in range(20):
		# 	self.columnconfigure(i, weight=1)
		# Title:
		row = 0
		titleMessage = Message(self, text="Annotator", **self.headMessagesOption)
		titleMessage.grid(row=row, column=11, sticky='ewn')
		row += 1
		# Description:
		w1 = Message(self, text=self.toolDescription, width=200, font='Arial 8')
		w1.grid(sticky="n", row=row, column=11, pady=4)
		row += 1
		# Task description:
		if self.taskDescription is not None:
			titleMessage = Message(self, text="Task description", **self.headMessagesOption)
			titleMessage.grid(row=row, column=11, sticky='ewn')
			row += 1
			w1 = Message(self, text=self.taskDescription, width=200, font='Arial 8')
			w1.grid(sticky="n", row=row, column=11, pady=4)
			row += 1
		# Labels:
		self.labelFrameRow = row
		row += 1
		# Control head:
		browsingMessage = Message(self, text="Browsing", **self.headMessagesOption)
		browsingMessage.grid(row=row, column=11, sticky='ewn')
		row += 1
		# We make buttons:
		subFrame = Frame(self)
		subFrame.columnconfigure(0, weight=1)
		subFrame.columnconfigure(1, weight=1)
		subFrame.rowconfigure(0, weight=1)
		subFrame.grid(row=row, column=11, sticky="new")
		previousButton = Button(subFrame, text="<<", command=self.left)
		previousButton.grid(row=0, column=0, sticky="new")
		nextButton = Button(subFrame, text=">>", command=self.right)
		nextButton.grid(row=0, column=1, sticky="new")
		row += 1


	def left(self):
		print("a")
		try:
			self.leftCallback()
		except Exception as e:
			logException(e, self)

	def right(self):
		print("b")
		try:
			self.rightCallback()
		except Exception as e:
			logException(e, self)
		
		
			  

def main():
  
	root = Tk()
	root.geometry("1200x800+600+100")
	app = AnnotatorUI()
	root.mainloop()
	

if __name__ == '__main__':
	main() 