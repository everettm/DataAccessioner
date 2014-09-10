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
Dependency: accession_settings.txt  
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
This program will overwrite original filenames and the given directory's subdirectory names, which are stored in "renames.csv" of each bag's "meta" folder. However, Python has issues reading ®, ©, ™, and , so the original names of files and folders containing these characters are not preserved in renames.csv.     
The [BagIt](http://en.wikipedia.org/wiki/BagIt) bag file creation aspect was removed but the structure remains (/top_dir/bag/data/). To bag everything, run BagIt on individual folders or [bagbatch](https://wiki.carleton.edu/display/carl/Bagit) on the top_dir directory.

#### Known Issues:
- _Debug option not always working as it should_. It cannot make a complete copy of the directory if there are folders or files with invalid characters (for windows - test this on a mac?). Tried using os, shutil, and glob. Glob is the best bet, as it can use wildcards, but didn't manage to write a successful implementation. However, WITHOUT using the debug option this problem can be avoided and the accessioner works just fine on these files (and also removes these particular invalid characters)  
- _Filenames and extensions_. Filenames with a period are being confused for extensions - not sure if there's any way to fix this. Considered using http://filext.com/ to check whether or not a file extension exists, but there are all sorts of extensions. In particular, I was looking to eliminate the false number extensions DA finds, but extensions like '.000' and '.3' are real extensions. Using the database might eliminate -some- extensions, but not enough to make a complete implementation of this viable  
