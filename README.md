dataAccessioner.py
Code by Mikenna Everett
mikenna.everett@gmail.com

Given a directory of folders to be accessioned, this program will create a "bag" structure for each folder, cleanse filenames, and 
	generate an import template for the bags.

Given a single file, this program will create a "bag" containing the single file and cleanse the filename.

USAGE INSTRUCTIONS:
Run this program at a command line terminal. In the terminal, navigate to the directory where accessioner.py is saved and run it with 1 argument:

"python accessioner.py [PATH]"

INPUT:

	Two options:

		-a full path to a directory OR a directory name in the same folder as accessioner.py. This directory should contain "bags" for accessioning.

		-a full path to a file OR a file name in the same folder as accessioner.py.
OUTPUT:

	-ImportTemplate.csv, an import template with information on each bag in the directory, stored in the directory (or, in the same directory as accessioner.py if a file was specified as the argument).

	-Bags with cleansed filenames. Each bag contains the following directories within it:

		-"dips"

		-"meta" (contains a .csv document with original file names, if applicable)

		-"originals" (contains all of the data that the beg held)

Notes: 

	-This program will overwrite original filenames, which are stored in "renames.csv" of each bag's "meta" folder.

	-the md5 hash for each directory is generated using the hashlib module in the medhod getDirectoryInfo_renameFiles(). After a chunk of a file is read using 
	hashlib.read(), the directory hash is updated using hashlib.update(). See getDirectoryInfo_renameFiles().

TODO:
	Append to previously existing rename files
	
	Add datestamp (plus index if needed) to the end of the import template file
	
	Checksum generating time tests
	
	Clean-up code (implement a class structure)