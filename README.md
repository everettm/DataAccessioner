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
- debug option cannot make a complete copy of the directory if there are folders or files with invalid characters (for windows - test this on a mac?). Tried using os, shutil, and glob. Glob is the best best, as it can use wildcards, but haven't gotten a working implementation yet. However, WITHOUT using the debug option this problem can be avoided and the accessioner works just fine on these files (and also removes these particular invalid characters)  
- filenames with a period are being confused for extensions - not sure if there's any way to fix this  
- when completely removing characters like ®, make sure that file/folder name doesn't already exist (similar to how cleanse_dict() uses path_already_exists())  
- consider adding deletechars from remove_special_characters() to accession_settings.txt  
