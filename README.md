Archives Data Accessioner
=========================

Code by Liam Everett liam.m.everett@gmail.com

General Description:
* Given a directory of folders to be accessioned, this program will create a bagit bag structure for each folder, cleanse filenames, and generate an import template for the bags.
* Given a single file or folder, this program will create a bag containing the single file and cleanse the filename.

Usage Instructions
------------------
Run this program with the following syntax:

`python data_accessioner.py <path>`

#### Input (2 options)
* A full path to a directory OR a directory name in the same folder as data_accessioner.py. This directory should contain files and "bags" for accessioning.
* A full path to a file OR a file name in the same folder as data_accessioner.py.

#### Output
* ImportTemplate_<date>.csv, an import template with information on each bag in the directory, stored in the directory (or, in the same directory as data_accessioner.py if a file was specified as the argument).
* Bags with cleansed filenames. Each bag contains the following directories within it:
	* /data/dips/
	* /data/meta/ (contains a .csv document with original file names, if applicable)
	* /data/originals/ (contains all of the data that the bag held originally)

Notes
-----
This program will overwrite original filenames, which are stored in "renames.csv" of each bag's "meta" folder.
Bagit link: https://wiki.carleton.edu/display/carl/Bagit

#### TODO:
-Directories containing numbers get stored with a weird identifier. Fix that?
-Make sure the bags follow the protocol outlined here: http://tools.ietf.org/html/draft-kunze-bagit-10#section-2.1
