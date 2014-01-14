Archives Data Accessioner
=========================

Code by Mikenna Everett mikenna.everett@gmail.com

General Description:
* Given a directory of folders to be accessioned, this program will create a "bag" structure for each folder, cleanse filenames, and 
	generate an import template for the bags.
* Given a single file, this program will create a "bag" containing the single file and cleanse the filename.

Usage Instructions
------------------
Run this program with the following syntax:

`python dataAccessioner.py <path>`

#### Input (2 options)
* A full path to a directory OR a directory name in the same folder as dataAccessioner.py. This directory should contain "bags" for accessioning.
* A full path to a file OR a file name in the same folder as dataAccessioner.py.

#### Output
* ImportTemplate.csv, an import template with information on each bag in the directory, stored in the directory (or, in the same directory as dataAccessioner.py if a file was specified as the argument).
* Bags with cleansed filenames. Each bag contains the following directories within it:
	* /data/dips/
	* /data/meta/ (contains a .csv document with original file names, if applicable)
	* /data/originals/ (contains all of the data that the bag held originally)

Notes
-----
This program will overwrite original filenames, which are stored in "renames.csv" of each bag's "meta" folder.

The md5 hash for each directory is generated using the hashlib module in the medhod getDirectoryInfo_renameFiles(). After a chunk of a file is read using 
	hashlib.read(), the directory hash is updated using hashlib.update(). See getDirectoryInfo_renameFiles().

#### TODO:
* Checksum generating time tests
* Clean up code (implement a class structure)