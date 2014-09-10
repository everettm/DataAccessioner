#!/usr/bin/env python
# -*- coding: utf-8 -*-
''' 
data_accessioner.py
Code by Liam Everett
liam.m.everett@gmail.com
    Edited by Sahree Kasper
    sahreek@gmail.com
'''
import ast
import csv
import datetime, time
import os, sys, platform
import re, string
import shutil

class DataAccessioner:
    def __init__(self,settings_file):
        self.excludes, self.excludes_regex = [], []
        self.storage_location_name = ""
        self.initialize_accession_settings(settings_file)

        # Regular expressions for file name cleansing
        self.valid_bag_name_format = re.compile("^[0-9]{8}_[0-9]{6}_.*")
        self.chars_to_remove = re.compile("[:'\+\=,\!\@\#\$\%\^\&\*\(\)\]\[ \t]") # non-Windows
        if sys.platform == 'win32' or platform.system() == 'Windows':
            self.chars_to_remove = re.compile("[:/'\+\=,\!\@\#\$\%\^\&\*\(\)\]\[ \t]")

        self.import_file_name, self.import_file, self.import_writer = None, None, None
        self.import_header = ["Month", "Day", "Year", "Title", "Identifier", "Inclusive Dates", 
        "Received Extent", "Extent Unit", "Processed Extent", "Extent Unit", "Material Type", 
        "Processing Priority", "Ex. Comp. Mont", "Ex. Comp. Day", "Ex. Comp. Year", "Record Series", 
        "Content", "Location", "Range", "Section", "Shelf", "Extent", "ExtentUnit", "CreatorName", 
        "Donor", "Donor Contact Info", "Donor Notes", "Physical Description", "Scope Content", "Comments"]

        # Will keep track of values for the current bag's row in the import file
        self.import_row = {val:"" for val in self.import_header}
        # Fields that are consistent in every row of the import file
        self.now = datetime.datetime.now()
        self.import_row["Month"] = self.now.month
        self.import_row["Day"] = self.now.day
        self.import_row["Year"] = self.now.year
        self.import_row["Location"] = self.storage_location_name
        self.import_row["Extent Unit"], self.import_row["ExtentUnit"] = "Gigabytes", "Gigabytes"

    def initialize_accession_settings(self, settings_file):
        """ Parses accession_settings.txt to set self.excludes, self.excludes_regex,
        and self.storage_location_name """        
        with open(settings_file) as f:
            for line in f:
                if line.startswith('EXCLUDES'):
                    self.excludes = line.strip('EXCLUDES: ').strip('\n').split(', ')
                if line.startswith('EXCLUDE_REGEX'):
                    self.excludes_regex = [re.compile(line.strip('EXCLUDE_REGEX: ').strip('\n'))]
                if line.startswith('STORAGE_LOCATION_NAME'):
                    self.storage_location_name = line.strip('STORAGE_LOCATION_NAME:').strip(' ').strip('\n')

    def initialize_import_file(self, top_dir):
        """ If self.create_import_file is True (set to False when calling accession_bags_
        in_dir()), an import file (for archon database) for the bags will be created. """
        if self.create_import_file:
            self.import_file_name = r"ImportTemplate_%s%02d%02d" % (self.now.year, self.now.month, self.now.day)
            self.import_file_name = path_already_exists(os.path.join(top_dir,self.import_file_name + '.csv'))
            self.import_file = open(os.path.join(top_dir,self.import_file_name),'wb')
            self.import_writer = csv.writer(self.import_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            self.import_writer.writerow(self.import_header)

    def accession_bags_in_dir(self, top_dir, import_file = True):
        """ Begins accessioning all items (files and folders/bags) using the given 
        directory, top_dir, and creates an import file. """
        # If self.create_import_file is set to false, no import file will be made
        self.create_import_file = import_file
        self.initialize_import_file(top_dir)

        print "accessioning...\n-----"
        # creates a list of paths to all files and folders/bags in top_dir
        path_list = [x for x in os.listdir(ast.literal_eval("u'" + (top_dir.replace("\\", "\\\\")) + "'")) if x != self.import_file_name]
        for bag in path_list:
            full_bag_path = os.path.join(top_dir,bag)
            # prevents the import file from being bagged
            if re.sub(r'_\d+', '', full_bag_path) == re.sub(r'_\d+', '', self.import_file_name):
                continue
            if not self.is_excluded(bag):
                print "current bag:", bag, "\n"
                if not os.path.isdir(full_bag_path):
                    bag = self.accession_file(full_bag_path)
                else:
                    bag = self.accession_bag(full_bag_path)
                print "accessioning complete for", bag, "\n-----"
            else:
                print 'not accessioning', bag, "based on accession settings \n-----"
        print "done"

    def accession_file(self, file_path):
        """ Given the path to a file, creates a new folder (or "bag"), to hold it 
        and calls accession_bag(p) where p is the newly created bag containing the 
        file. """
        new_directory = os.path.splitext(file_path)[0]
        new_directory = new_directory.encode('cp850', errors='ignore')

        # If new name is in use, add an index
        new_directory = path_already_exists(new_directory)
    
        os.mkdir(new_directory)
        os.rename(file_path,os.path.join(new_directory, os.path.basename(file_path)))
        file_path = new_directory

        return self.accession_bag(os.path.splitext(file_path)[0])

    def accession_bag(self, bag_path):
        """ Given the path to a directory:
        1. format bag name: creates timestamp and adds it to the bag's name
        2. creates the bag structure (bag/data/: dips, meta, originals)
        3. cleanse bag name: replaces special characters
        4. create relative bag dictionary: makes dictionary of files and paths, 
            checks if paths need renaming
        5. cleanse dict: renames files
        6. write rename file
        7. traverse bag contents: returns the size, extensions, and # of files """
        # update self.now ONCE per bag, sleep prevents duplicate names
        time.sleep(1)
        self.now = datetime.datetime.now()
        bag_path, identifier = self.format_bag_name(bag_path)
        self.create_bag_structure(bag_path)

        bag_path, bags_renamed = self.cleanse_bag_name(bag_path) # Remove special characters
        
        # cleanse filenames in bag, save rename.csv metadata
        b_dict, needs_renaming = self.create_relative_bag_dict(bag_path,os.path.join(bag_path,"data","originals"),0)

        if needs_renaming or bags_renamed:
            renamed_files_list = self.cleanse_dict(os.path.join(bag_path,"data"),b_dict,0)
            self.write_rename_file(bag_path,renamed_files_list,bags_renamed)
        
        if self.create_import_file:
            size, extensions, num_files = self.traverse_bag_contents(bag_path)
            bag_name = os.path.basename(bag_path)
            self.import_row["Title"] = bag_name
            self.import_row["Identifier"] = identifier
            self.import_row["Received Extent"] = size
            self.import_row["Processed Extent"] = size
            self.import_row["Content"] = bag_name
            self.import_row["Scope Content"] = bag_name
            self.import_row["Extent"] = size
            self.import_row["Physical Description"] = "Extensions include: " + "; ".join(extensions)

            # Writes data to the import template
            new_row = []
            for item in self.import_header:
                new_row.append(self.import_row[item])
            self.import_writer.writerow(new_row)

        return os.path.basename(bag_path)

    def format_bag_name(self, bag_path):
        """ Renames the bag (folder) in the format yyyyddmm_hhmmss_originalDirTitle, 
        created from the self.now timestamp. Returns the new bag path and the 
        date_created, to be used as the bag's identifier. """
        bag_name = os.path.basename(bag_path)

        date_created = "%s%02d%02d_%02d%02d%02d" % (self.now.year, self.now.day, \
        self.now.month, self.now.hour, self.now.minute, self.now.second)

        if self.valid_bag_name_format.search(bag_name) == None:
            new_valid_bag_name = "%s_%s" % (date_created,bag_name)
            os.rename(bag_path,os.path.join(os.path.dirname(bag_path),new_valid_bag_name))
            bag_path = os.path.join(os.path.dirname(bag_path),new_valid_bag_name)

        return bag_path, date_created

    def create_bag_structure(self, bag_path):
        """ Given the bag path, creates necessary directories and moves pre-existing 
        directories to the correct place. 
        Structure: bag/data, bag/data/dips, bag/data/meta, bag/data/originals """
        # files_in_bag: if folders are already created but may need to be moved
        files_in_bag = set(os.listdir(bag_path))
        
        # If directory bag/data is not present, create it
        if not os.path.exists(os.path.join(bag_path,"data")):
            os.mkdir(os.path.join(bag_path,"data"))
        else:
            files_in_bag.remove("data")

        for dir_type in ["dips", "meta", "originals"]:
            # If these directories are present in bag/: bag/dips, bag/meta or bag/originals move them to bag/data/
            if os.path.exists(os.path.join(bag_path, dir_type)):
                shutil.move(os.path.join(bag_path, dir_type),os.path.join(bag_path, dir_type))
                files_in_bag.remove(dir_type)
            # If bag/data/originals, dips, or meta are not present in bag/data, create them
            if not os.path.exists(os.path.join(bag_path, "data", dir_type)):
                os.mkdir(os.path.join(bag_path, "data", dir_type))

        # Move all other files in bag to bag/data/originals
        for f in files_in_bag:
            if not self.is_excluded(f):
                shutil.move(os.path.join(bag_path,f), os.path.join(bag_path,"data","originals"))

    def is_excluded(self, filename):
        """ Returns True if the file name corresponds with a file name in 
        self.excludes or if the file name has any of the characters in 
        self.excludes_regex. Both excludes and excludes_regex are defined in 
        accession_settings.txt. Returns False otherwise. """
        if filename in self.excludes:
            return True
        for pattern in self.excludes_regex:
            # bad file name
            if pattern.search(filename) != None:
                return True
        return False

    def cleanse_bag_name(self, bag_path):
        """ Removes special characters listed in self.chars_to_remove from the given
        bag and replaces them with underscores. """
        replacement_path = bag_path
        bag_name = os.path.basename(bag_path)
        replacement_name = bag_name

        for match in self.chars_to_remove.finditer(bag_name):
            replacement_name = replacement_name.replace(match.group(), "_")
        replacement_name = replacement_name.encode('cp850', errors='ignore')

        if replacement_name != bag_name:
            replacement_path = os.path.join(os.path.dirname(bag_path),replacement_name)
            os.rename(bag_path, replacement_path)

        bags_renamed = []
        if bag_name != replacement_name:
            bags_renamed.append(bag_name)
            bags_renamed.append(replacement_name)
        return (replacement_path, bags_renamed)

    def create_relative_bag_dict(self, walk_path,path_to_originals,depth):
        """ Given a bag with proper hierarchical structure, returns:
                -a dictionary with the proper relative paths for files.
                -a boolean value for whether anything needs to be renamed. """
        needs_renaming = False
        sub_dict = {}
        return_path = os.path.basename(walk_path)
        if depth > 2:
            return_path = os.path.relpath(walk_path, os.path.dirname(path_to_originals))
        return_dict = {return_path:sub_dict}

        dirs_list, files_list = os.walk(walk_path).next()[1], os.walk(walk_path).next()[2]
        for name in files_list:
            if self.chars_to_remove.search(name) != None or name.encode('cp850', errors='ignore') != name:
                needs_renaming = True
            if depth < 2:
                sub_dict[name] = ""
            else:
                sub_dict[os.path.relpath(os.path.join(walk_path,name), os.path.dirname(path_to_originals))] = ""
        for name in dirs_list:
            if self.chars_to_remove.search(name) != None or name.encode('cp850', errors='ignore') != name:
                needs_renaming = True
            sub_sub_dict, sub_rename = self.create_relative_bag_dict(os.path.join(walk_path,name),path_to_originals,depth+1)
            if sub_rename:
                needs_renaming = True
            new_key = sub_sub_dict.keys()[0]
            sub_dict[new_key] = sub_sub_dict[new_key]
        return return_dict, needs_renaming
    
    def cleanse_dict(self, path_to_data, bag_dict, depth):
        """ Renames files (does this include folders?) as needed and returns a list 
        of the renamed files. """
        renamed_files_list = []
        for item in bag_dict:
            repl_str = item.encode('cp850', errors='ignore')
            for match in self.chars_to_remove.finditer(item):
                repl_str = repl_str.replace(match.group(), "_")

            # Rename the file if necessary, remove the previous key in place of the new one
            if os.path.basename(repl_str) != os.path.basename(item):
                repl_str = path_already_exists(os.path.join(path_to_data,repl_str))
                # Rename the file, update dictionary and renames list
                old_name = os.path.join(path_to_data,os.path.dirname(repl_str),os.path.basename(item))
                os.rename(old_name, os.path.join(path_to_data,repl_str))
                bag_dict[repl_str] = bag_dict[item]
                renamed_files_list.append(list((os.path.basename(item.encode('cp850', errors='replace')), os.path.basename(repl_str))))
                del bag_dict[item]

            # renames all subdirectories and files
            if repl_str in bag_dict:
                if bag_dict[repl_str] != "":
                    renamed_files_list = renamed_files_list + (self.cleanse_dict(path_to_data,bag_dict[repl_str], depth+1))
        return renamed_files_list

    def write_rename_file(self, bag_path, files_to_rename, bags_renamed):
        """ Writes a csv file with the files and folders that have been renamed, 
        allowing us to go back to this file and browse the original names. """
        rename_file_path = os.path.join(bag_path,"data","meta","renames")

        if os.path.exists(rename_file_path + ".csv"):
            out_file = open(rename_file_path + ".csv", "ab")
            writer = csv.writer(out_file, quoting=csv.QUOTE_ALL)
        else:
            out_file = open(rename_file_path + ".csv", "wb")
            writer = csv.writer(out_file, quoting=csv.QUOTE_ALL)
            writer.writerow(["Old_Name", "New_Name", "Date Renamed"])

        if len(bags_renamed) == 2:
            bags_renamed.append(str(self.now))
            writer.writerow(bags_renamed)

        for row in files_to_rename:
            row[0], row[1] = os.path.basename(row[0]), os.path.basename(row[1])
            row.append(str(self.now))
            writer.writerow(row)

        out_file.close()

    def traverse_bag_contents(self, bag_path):
        """ Returns the size, extensions, and the number of files. num_files is not 
        currently in use. """
        total_size = 0
        file_types = set()
        num_files = 0
        # List of everything contained in the "originals" folder (the primary bag data we're concerned with)
        dirs_in_originals_folder = os.listdir(os.path.join(bag_path, "data", "originals"))

        for root, dirs, files in os.walk(ast.literal_eval("u'" + (bag_path.replace("\\", "\\\\")) + "'"), topdown=False):
            if os.path.basename(root) == "originals":
                num_files += len(files) + len(dirs)
            elif os.path.basename(root) in dirs_in_originals_folder:
                dirs_in_originals_folder = dirs_in_originals_folder + os.listdir(root)
                # in case a directory after we've traversed originals has the same name as a directory in originals
                dirs_in_originals_folder.remove(os.path.basename(root))
                num_files+=len(files) + len(dirs)
            for names in files:
                filepath = os.path.join(root,names)
                if not self.is_excluded(names):
                    total_size += os.path.getsize(filepath)
                    if not os.path.splitext(filepath)[1] == "":
                        file_types.add(os.path.splitext(filepath)[1])

        return self.convert_size_to_string(total_size), file_types, num_files

    def convert_size_to_string(self, size):
        """ If size < 0.01, return 0.01. Otherwise convert size into a truncated string. """
        conversion = 9.31323e-10
        file_size = size * conversion
        file_size_string = "%.2f" % file_size
        if file_size < 0.01:
            file_size_string = "0.01"    
        return file_size_string

def rec_traverse_dir(curr_dir, root):
    """ Recursive function to traverse the current directory. Used in 
    conjunction with remove_special_characters() to remove all invalid values 
    from all subdirectories and their files. See http://stackoverflow.com/a/13528334/3889452"""
    try :
        dfList = [os.path.join(curr_dir, f_or_d) for f_or_d in os.listdir(curr_dir)]
    except:
        print "wrong path name/directory name"
        return

    for file_or_dir in dfList:
        rename = remove_special_characters(file_or_dir)
        if os.path.isfile(file_or_dir):
            try:
                shutil.move(os.path.join(root, file_or_dir),os.path.join(root, rename))
            except:
                print 'error with file', os.path.join(root, file_or_dir)
        if os.path.isdir(file_or_dir):
            try:
                # if statement: renames name w/ bad chars, and if rename exists 
                # already, gives it an index. Not perfect.       
                if not os.path.join(root, rename) == file_or_dir:
                    rename = path_already_exists(os.path.join(root, rename))
                shutil.move(os.path.join(root, file_or_dir), os.path.join(root, rename))
                rec_traverse_dir(rename, root)
            except:
                print 'error with folder', os.path.join(root, file_or_dir)

def remove_special_characters(value):
    """ Given a string, removes the characters specified in deletechars and returns 
    the new value. At the time of writing, the primary goal was to remove registered 
    trademark symbols (®) from path names. Python and Windows CMD (not sure about 
    Unix) cannot interpret these symbols, and so, the loss of these symbols is not 
    reflected in renames.csv. The header: 
        #!/usr/bin/env python 
        # -*- coding: utf-8 -*-
    is necessary for python to interpret these symbols. 
    See http://stackoverflow.com/q/1033424/3889452 and http://stackoverflow.com/a/25067408/3889452 """
    deletechars = '®©™'
    for c in deletechars:
        value = value.replace(c, '')
    return value

def path_already_exists(path):
    """ Given a path, checks to see if it already exists. If it does, a new 
    with a corresponding index number (that counts up) is returned, such as 
    "/New_Folder_2". If it doesn't, the original path is returned. """
    if os.path.exists(path):
        ext = ''
        if os.path.isfile(path):
            path, ext = os.path.splitext(path)
        rename_index = 1
        while os.path.exists(path + "_" + str(rename_index) + ext):
            rename_index += 1
        path = path + "_" + str(rename_index) + ext
    return path 

def usage_message():
    print "\ndata_accessioner: Cleanses a directory of files/dirs and places them in properly-formatted bags using the bagit specifications.\
    \n\nUsage:\
            \n\tpython data_accessioner.py [options] <path>\
            \n\tpython data_accessioner.py -h | --help\
        \n\nOptions:\
            \n\t-h --help\tShow this screen.\
            \n\t-d --debug\tCreates a new copy of the original folder or file.\
        \n\nDependency:\
            \n\taccession_settings.txt"

def main():
    accessioner = DataAccessioner('accession_settings.txt')

    if len(sys.argv) <= 1 or len(sys.argv) > 3 or sys.argv[1] == "-h" or sys.argv[1] == "--help":
        return usage_message()
    elif len(sys.argv) == 2:
        path_arg = sys.argv[1]
    elif len(sys.argv) == 3:
        path_arg = sys.argv[2]

    if os.path.exists(path_arg):
        if sys.argv[1] == "-d" or sys.argv[1] == "--debug":
            timestamp = "_%s%02d%02d_%02d%02d%02d" % (accessioner.now.year, accessioner.now.day, \
            accessioner.now.month, accessioner.now.hour, accessioner.now.minute, accessioner.now.second)
            shutil.copytree(path_arg, path_arg + timestamp)
            path_arg = path_arg + timestamp

        rec_traverse_dir(path_arg, path_arg)
        accessioner.accession_bags_in_dir(path_arg)

    else:
        print '\n<path> does not exist',
        usage_message()

main()