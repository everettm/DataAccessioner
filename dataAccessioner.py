''' 
dataAccessioner.py
Code by Mikenna Everett
mikenna.everett@gmail.com
'''
import os
import sys
import datetime
import csv
import re
import shutil
import ast
import hashlib

# Global regular expressions for use in various methods
validBagName = re.compile("^[0-9]{8}_.+")
validBagNameWithIndex = re.compile("^[0-9]{8}_[0-9]+_")
dateInValidBagName = re.compile("[0-9]{8}")
dateInValidBagNameWithIndex = re.compile("[0-9]{8}(_[0-9]+(?=_))?")
charsToRemove = re.compile("[:/'\+\=,\-\!\@\#\$\%\^\&\*\(\)\]\[ \t]")

# Global dictionaries for use in various methods
originalFileNames = {} # entries will be of the form {newName:oldName}
originalDirectoryNames = {} # entries will be of the form {newName:oldName}
dateDict = {} # entries will be of the form {date:count}
firstDateDict = {} # entries will be of the form {date:filename}


# Remove special characters from directory names. Remove any indexing if it exists.
def cleanseDirectoryName(fullPath, directoryName):
	replaceName = directoryName
	replacePath = fullPath

	for match in charsToRemove.finditer(directoryName):
		replaceName = replaceName.replace(match.group(), "_")

	replaceName = replaceName.encode('cp850', errors='ignore')

	if validBagNameWithIndex.search(replaceName) != None:
		startIndex = replaceName.index("_")
		endIndex = validBagNameWithIndex.search(replaceName).span()[1]
		replaceName = replaceName[:startIndex] + replaceName[endIndex-1:]

	if replaceName != directoryName:
		replacePath = os.path.join(os.path.dirname(fullPath),replaceName)
		originalDirectoryNames[replaceName] = directoryName
		os.rename(fullPath, replacePath)
	
	return replacePath, replaceName

# Format the bag names so that they have the proper name structure
def insertDateWithIndexNumber(fileName, filePath, dateDict, firstDateDict):
	date = dateInValidBagName.search(fileName).group()

	returnString = fileName

	date_indexed = date
	if date in dateDict:
		indexNum = dateDict[date] + 1
		date_indexed = date + "_" + str(indexNum)
		toRename = fileName.replace(date, date_indexed)
		os.rename(os.path.join(filePath,fileName),os.path.join(filePath,toRename))
		returnString = toRename
		
		if fileName in originalDirectoryNames:
			originalName = originalDirectoryNames[fileName]
			originalDirectoryNames[toRename] = originalName
			del originalDirectoryNames[fileName]
		else:
			originalDirectoryNames[toRename] = fileName
		if originalDirectoryNames[toRename] == toRename:
			del originalDirectoryNames[toRename]

		dateDict[date] = indexNum
	else:
		dateDict[date] = 1
		firstDateDict[date] = os.path.join(os.path.basename(filePath),fileName)

	return returnString

# Remove special characters from filenames.
def cleanseName(dirpath,f,bagName):

	# This is the case when cleanseName is called when the user has specified a directory of bags
	if os.path.isdir(dirpath):
		# cleanse filename, return the full path of the cleansed filename.
		replacement = f
		for match in charsToRemove.finditer(f):
			replacement = replacement.replace(match.group(),"_")
		replacement = replacement.encode('cp850', errors='ignore')

		if replacement != f:
			checkForOrig = os.path.split(dirpath)
			relativePath = os.path.relpath(dirpath,bagName)
			originalFileNames[os.path.join(relativePath,replacement)] = os.path.join(relativePath,f)

		fp = os.path.join(dirpath,f)
		os.rename(fp,os.path.join(dirpath,replacement))
		fp = os.path.join(dirpath,replacement)
		return fp
	
	# This is the case when cleanseName is called when the user has specified a single file
	else:
		replacement = f
		for match in charsToRemove.finditer(f):
			replacement = replacement.replace(match.group(),"_")
		replacement = replacement.encode('cp850', errors='ignore')

		replacePath = os.path.join("originals", replacement)
		
		if replacement != f:
			originalFileNames[replacePath] = os.path.join("originals", f)
			os.rename(dirpath, os.path.join(os.path.dirname(dirpath), replacement))
			
		return os.path.join(os.path.dirname(dirpath), replacement)

# Return an md5 checksum, size in bytes, and list of extensions for the given directory.
def getDirectoryInfo_renameFiles(directory, verbose=0):
	md5Hash = hashlib.md5() # Will be the hash returned for the directory.

	total_size = 0
	fileTypes = set()
	i = 0
	dirsInOriginals = []

	try:
		for root, dirs, files in os.walk(ast.literal_eval("u'" + (directory.replace("\\", "\\\\")) + "'"), topdown=False):
			if charsToRemove.search(os.path.basename(root)) != None:
				root = cleanseName(os.path.dirname(root),os.path.basename(root),directory)
			if os.path.basename(root) == "originals":
				dirsInOriginals = os.listdir(root)
				i+=len(files) + len(dirs)
			elif os.path.basename(root) in dirsInOriginals:
				dirsInOriginals = dirsInOriginals + os.listdir(root)
				# in case a directory after we've traversed originals has the same name as a directory in originals
				dirsInOriginals.remove(os.path.basename(root))
				i+=len(files) + len(dirs)
			for names in files:
				if verbose == 1:
					print 'Hashing', names
				filepath = cleanseName(root,names,directory)
				try:
					f1 = open(filepath, 'rb')
					a = True
					while a:
						# Read file in as little chunks and update md5Hash with each new chunk.
						buf = f1.read(4096)
						if not buf : break
						md5Hash.update(hashlib.md5(buf).hexdigest())
					f1.close()
				except:
					pass
				total_size += os.path.getsize(filepath)
				extType = os.path.splitext(filepath)[1]
				fileTypes.add(extType)
	except:
		import traceback
		# Print the stack traceback
		traceback.print_exc()
		sys.exit()

	return md5Hash.hexdigest(), total_size, fileTypes, i

# Return a boolean which is true when the given dictionary is empty.
def dictIsEmpty(myDict):
	for entry in myDict.keys():
		return False
	return True

def main():

	topDir = sys.argv[1]

	if len(sys.argv) > 2:
			args = sys.argv[1:]
			topDir = " ".join(args)

	curDirect = os.getcwd()

	if os.path.isdir(os.path.join(curDirect, topDir)) or os.path.isfile(os.path.join(curDirect, topDir)):
		topDir = os.path.join(curDirect, topDir)

	if not os.path.isdir(topDir) and not os.path.isfile(topDir):
		print "Error: invalid path"
		sys.exit()

	now = datetime.datetime.now()

	outFile = ""
	outTitle = "ImportTemplate" + "_" + str(now.year) + str(now.month) + str(now.day)
	if os.path.isdir(topDir):
		i = ""
		if os.path.exists(os.path.join(topDir,outTitle + '.csv')):
			outTitle = outTitle + "_"
			i = 1
			while os.path.exists(os.path.join(topDir,outTitle + str(i) + '.csv')):
				i+=1
		outFile = open(os.path.join(topDir,outTitle + str(i) + '.csv'),'wb')
	else:
		outFile = open('ImportTemplate.csv','wb')

	header = ["Month", "Day", "Year", "Title", "Identifier", "Inclusive Dates", "Received Extent", "Extent Unit", "Processed Extent", "Extent Unit", "Material Type", "Processing Priority", "Ex. Comp. Mont", "Ex. Comp. Day", "Ex. Comp. Year", "Record Series", "Content", "Location", "Range", "Section", "Shelf", "Extent", "ExtentUnit", "CreatorName", "Donor", "Donor Contact Info", "Donor Notes", "Physical Description", "Scope Content", "Comments"]
	writer = csv.writer(outFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
	writer.writerow(header)

	importRow = {}
	for item in header: 
		importRow[item] = ""

	importRow["Month"] = now.month
	importRow["Day"] = now.day
	importRow["Year"] = now.year
	importRow["Extent Unit"] = "Gigabytes"
	importRow["Location"] = "Archives Network Storage"
	importRow["ExtentUnit"] = "Items"

	# ACCESSION A DIRECTORY OF "BAGS"
	if os.path.isdir(topDir):
		thePathList = os.listdir(ast.literal_eval("u'" + (topDir.replace("\\", "\\\\")) + "'"))
		print "accessioning..."
		for direc in thePathList:
			d = os.path.join(topDir,direc) # full path
			if os.path.isdir(d):
				print "accessioning", d
				filesInDir = set(os.listdir(d))

				# If directory bag/data is not present, create it.
				if not os.path.exists(os.path.join(d,"data")):
					os.mkdir(os.path.join(d,"data"))
				else:
					filesInDir.remove("data")

				# If any of these directories are present: bag/dips, bag/meta and bag/originals - move them to bag/data.
				if os.path.exists(os.path.join(d,"dips")):
					shutil.move(os.path.join(d,"dips"),os.path.join(d,"data"))
					filesInDir.remove("dips")

				if os.path.exists(os.path.join(d,"meta")):
					shutil.move(os.path.join(d,"meta"),os.path.join(d,"data"))
					filesInDir.remove("meta")

				if os.path.exists(os.path.join(d,"originals")):
					shutil.move(os.path.join(d,"originals"),os.path.join(d,"data"))
					filesInDir.remove("originals")

				# If bag/data/originals is not present, create it. If bag/data/dips is not present, create it. If bag/data/meta is not present, create it.
				if not os.path.exists(os.path.join(d,"data","originals")):
					os.mkdir(os.path.join(d,"data","originals"))

				if not os.path.exists(os.path.join(d,"data","dips")):
					os.mkdir(os.path.join(d,"data","dips"))

				if not os.path.exists(os.path.join(d,"data","meta")):
					os.mkdir(os.path.join(d,"data","meta"))

				# All other files in bag, move to bag/data/originals.
				for f in filesInDir:
					shutil.move(os.path.join(d,f), os.path.join(d,"data","originals"))

				# Cleanse the bag name (replace any odd characters with "_")
				d, direc = cleanseDirectoryName(d, direc)

				# Properly format the bag name
				if validBagName.search(direc) == None:
					dateCreated = "%s%02d%02d" % (datetime.datetime.fromtimestamp(os.path.getctime(d)).year, datetime.datetime.fromtimestamp(os.path.getctime(d)).month, datetime.datetime.fromtimestamp(os.path.getctime(d)).day)
					newValidBagName = "%s_%s" % (dateCreated,direc)
					if direc in originalDirectoryNames:
						orig = originalDirectoryNames[direc]
						originalDirectoryNames[newValidBagName] = orig
						del originalDirectoryNames[direc]
					else:
						originalDirectoryNames[newValidBagName] = direc
					os.rename(d,os.path.join(topDir, newValidBagName))
					d = os.path.join(topDir,newValidBagName)
					direc = newValidBagName

				direc = insertDateWithIndexNumber(direc,topDir,dateDict,firstDateDict)

				d = os.path.join(topDir,direc)

				importRow["Title"] = direc
				importRow["Content"] = direc
				importRow["Scope Content"] = direc

				identifierString = dateInValidBagNameWithIndex.search(direc).group().replace("_","/")
				identifierString = identifierString[0:4] + "-" + identifierString[4:6] + "-" + identifierString[6:]
				importRow["Identifier"] = identifierString
				
				# Here is where the os walk occurs.
				hashes, size, extensions, numFiles = getDirectoryInfo_renameFiles(d)
				
				importRow["Comments"] = "md5 hash: " + hashes
				importRow["Extent"] = str(numFiles)

				

				conversion = 9.31323e-10
				fileSize = size * conversion
				fileSizeString = "%.2f" % fileSize
				importRow["Received Extent"] = fileSizeString
				importRow["Processed Extent"] = fileSizeString
				
				extensionString = "Extensions include: "
				for item in extensions:
					if len(item) > 0:
						extensionString = extensionString + item + "; "
				if len(extensions) == 0:
					extensionString = ""
				else:
					extensionString = extensionString[:len(extensionString)-2]

				importRow["Physical Description"] = extensionString

				# write the rename file
				if not dictIsEmpty(originalFileNames) or not dictIsEmpty(originalDirectoryNames):
					ofString = os.path.join(topDir, direc, "data", "meta", "renames")
					if os.path.exists(ofString + ".csv"):
						name = False
						renameNum = 1
						ofString = ofString + str(renameNum)
						while not name:
							if os.path.exists(ofString + ".csv"):
								renameNum+=1
								ofString = ofString[:len(ofString) - 1] + str(renameNum)
							else:
								name = True
					outFile2 = open(ofString + ".csv","wb")
					writer2 = csv.writer(outFile2, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
					writer2.writerow(["New_Name", "Old_Name"])

					for entry in originalDirectoryNames.keys():
						rowToWrite = [entry,originalDirectoryNames[entry].encode('utf-8')]
						writer2.writerow(rowToWrite)

					for entry in originalFileNames.keys():
						rowToWrite = [entry,originalFileNames[entry].encode('utf-8')]
						writer2.writerow(rowToWrite)

					outFile2.close()
					originalFileNames.clear()
					originalDirectoryNames.clear()

				# write the import template row
				newRow = []
				for item in header:
					newRow.append(importRow[item])
				writer.writerow(newRow)
		print "done"
	
	# ACCESSION A SINGLE FILE
	else:
		print
		print "accessioning", os.path.basename(topDir), "...",
		topDir = cleanseName(topDir, os.path.basename(topDir),"")
		dateCreated = "%s%02d%02d" % (datetime.datetime.fromtimestamp(os.path.getctime(topDir)).year, datetime.datetime.fromtimestamp(os.path.getctime(topDir)).month, datetime.datetime.fromtimestamp(os.path.getctime(topDir)).day)
		newValidBagName = "%s_%s" % (dateCreated,os.path.basename(os.path.splitext(topDir)[0]))
		newBag = os.path.join(os.getcwd(),newValidBagName)
		if not os.path.exists(newBag):
			os.mkdir(newBag)
			newBag = os.path.join(newBag,"data")
			os.mkdir(newBag)
			os.mkdir(os.path.join(newBag,"originals"))
			os.mkdir(os.path.join(newBag,"meta"))
			os.mkdir(os.path.join(newBag,"dips"))
		else:
			writeOver = raw_input("\nWarning: the bag " + newValidBagName + " already exists. Overwrite? [Y/N] ")
			if writeOver[0] == "Y" or writeOver[0] == "y":
				shutil.rmtree(newBag)
				os.mkdir(newBag)
				newBag = os.path.join(newBag,"data")
				os.mkdir(newBag)
				os.mkdir(os.path.join(newBag,"originals"))
				os.mkdir(os.path.join(newBag,"meta"))
				os.mkdir(os.path.join(newBag,"dips"))
			else:
				print "Will not overwrite. Exiting"
				sys.exit()

		importRow["Content"] = newValidBagName
		importRow["Identifier"] = dateCreated
		conversion = 9.31323e-10
		size = os.path.getsize(topDir)
		fileSize = size * conversion
		fileSizeString = "%.2f" % fileSize
		importRow["Received Extent"] = fileSizeString
		importRow["Processed Extent"] = fileSizeString
		importRow["Comments"] = "Extensions include: " + os.path.splitext(topDir)[1]
		shutil.move(topDir, os.path.join(newBag,"originals"))

		if not dictIsEmpty(originalFileNames):
			outFile2 = open(os.path.join(newBag, "meta", "renames.csv"), "wb")
			writer2 = csv.writer(outFile2, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
			writer2.writerow(["New_Name", "Old_Name"])

			for entry in originalFileNames.keys():
				rowToWrite = [entry,originalFileNames[entry].encode('utf-8')]
				writer2.writerow(rowToWrite)
			outFile2.close()
		print "done"

	# write the import template row
		newRow = []
		for item in header:
			newRow.append(importRow[item])
		writer.writerow(newRow)

	outFile.close()

main()

