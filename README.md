Archives Data Accessioner
=========================

Code by Liam Everett liam.m.everett@gmail.com  
Edited by Sahree Kasper sahreek@gmail.com

General Description:
* Given a directory of folders to be accessioned, this program will create a bagit bag structure for each folder, cleanse filenames, and generate an import template for the bags.
* Given a single file or folder, this program will create a bag containing the single file and cleanse the filename.

Usage Instructions
------------------
Run this program with the following syntax:

`python data_accessioner.py [option] <path>`  
Options:  
-h, --help: shows help menu  
-d, --debug: creates a new copy of the original folder or file

#### Input
* A full path to a directory OR a directory name in the same folder as data_accessioner.py. This directory should contain files and "bags" for accessioning.

#### Output
* ImportTemplate_<date>.csv, an import template with information on each bag in the directory, stored in the directory (or, in the same directory as data_accessioner.py if a file was specified as the argument).
* Bags with cleansed filenames. Each bag contains the following directories within it:
	* /data/dips/
	* /data/meta/ (contains a .csv document with original file names and date changed, if applicable)
	* /data/originals/ (contains all of the data that the bag held originally)
* If the debug option is indicated, a new file or directory will be created with a timestamp ("_DD/MM/YY_HH/MM/SS") appended to its name.  

Notes
-----
This program will overwrite original filenames and the given directory's subdirectory names, which are stored in "renames.csv" of each bag's "meta" folder.    
The [BagIt](http://en.wikipedia.org/wiki/BagIt) bag file creation aspect was removed but the structure remains (/top_dir/bag/data/). To bag everything, run BagIt on individual folders or [bagbatch](https://wiki.carleton.edu/display/carl/Bagit) on the top_dir directory.

#### TODO:
- run more tests on different folders