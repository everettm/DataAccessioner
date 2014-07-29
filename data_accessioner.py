''' 
data_accessioner.py
Code by Liam Everett
liam.m.everett@gmail.compile
    Edited by Sahree Kasper
    sahreek@gmail.com
'''
import os
import sys
import datetime, time
import csv
import re
import shutil
import ast

class DataAccessioner:
    def __init__(self,settings_file):
        self.excludes = []
        self.excludes_regex = []
        self.storage_location_name = ""
        self.initialize_accession_settings(settings_file)

        # Regular expressions for file name cleansing
        # Bag format: yyyyddmm_hhmmss_originalDirTitle
        self.valid_bag_name_format = re.compile("^[0-9]{8}_[0-9]{6}_.*")
        self.chars_to_remove = re.compile("[:/'\+\=,\!\@\#\$\%\^\&\*\(\)\]\[ \t]") # characters to remove from files

        # Global dictionaries/lists for use in various methods
        self.original_file_names = {} # entries will be of the form {new_name:old_name}

        self.import_file_name = None
        self.import_file = None
        self.import_writer = None
        self.import_header = ["Month", "Day", "Year", "Title", "Identifier", \
        "Inclusive Dates", "Received Extent", "Extent Unit", "Processed Extent", \
        "Extent Unit", "Material Type", "Processing Priority", "Ex. Comp. Mont", \
        "Ex. Comp. Day", "Ex. Comp. Year", "Record Series", "Content", "Location", \
        "Range", "Section", "Shelf", "Extent", "ExtentUnit", "CreatorName", "Donor", \
        "Donor Contact Info", "Donor Notes", "Physical Description", "Scope Content", \
        "Comments"]

        # Will keep track of values for the current bag's row in the import file
        self.import_row = {val:"" for val in self.import_header}
        # Fields that are consistent in every row of the import file
        self.now = datetime.datetime.now()
        self.import_row["Month"] = self.now.month
        self.import_row["Day"] = self.now.day
        self.import_row["Year"] = self.now.year
        self.import_row["Extent Unit"] = "Gigabytes"
        self.import_row["Location"] = self.storage_location_name
        self.import_row["ExtentUnit"] = "Gigabytes"

    def initialize_accession_settings(self,settings_file):
        """ 
        Using accession_settings.txt, sets self.storage_location_name, self.excludes, and self.excludes_regex 
        """        
        with open(settings_file) as f:
            f_content = [line.rstrip() for line in f]
            parse_state = "none"
            for i in range(len(f_content)):
                line = f_content[i]
                if parse_state == "none":
                    if line == "EXCLUDES:":
                        parse_state = "excludes"
                    elif line[:23] == "STORAGE_LOCATION_NAME =":
                        self.storage_location_name = line[24:]
                    elif line == "EXCLUDE_REGEX:":
                        parse_state = "regex"
                elif parse_state == "excludes":
                    if line != "":
                        self.excludes.append(line)
                    else:
                        parse_state = "none"
                elif parse_state == "regex":
                    if line != "":
                        self.excludes_regex.append(line)
                    else:
                        parse_state = "none"
        for i in range(len(self.excludes_regex)):
            self.excludes_regex[i] = re.compile(self.excludes_regex[i])

    def initialize_import_file(self, top_dir):
        if self.create_import_file:
            file_name = r"ImportTemplate_%s%02d%02d" % (self.now.year, self.now.month, self.now.day)
            i = ""
            if os.path.exists(os.path.join(top_dir,file_name + '.csv')):
                file_name = file_name + "_"
                i = 1
                while os.path.exists(os.path.join(top_dir,file_name + str(i) + '.csv')):
                    i+=1
            self.import_file_name = file_name + str(i) + '.csv'
            self.import_file = open(os.path.join(top_dir,file_name + str(i) + '.csv'),'wb')
            self.import_writer = csv.writer(self.import_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            self.import_writer.writerow(self.import_header)

    def accession_bags_in_dir(self,top_dir, import_file):
        self.create_import_file = import_file
        self.initialize_import_file(top_dir)

        print "accessioning...\n-----"
        path_list = [x for x in os.listdir(ast.literal_eval("u'" + (top_dir.replace("\\", "\\\\")) + "'")) if x != self.import_file_name]
        for bag in path_list:
            if not self.is_excluded(bag):
                print "current bag:", bag, "\n"
                full_bag_path = os.path.join(top_dir,bag)
                if os.path.isdir(full_bag_path):
                    bag = self.accession_bag(full_bag_path)
                else:
                    bag = self.accession_file(full_bag_path)
                print "accessioning complete for", bag, "\n-----"
            else:
                print 'not accessioning', bag, "based on accession settings \n-----"
        print "done"

    def accession_bag(self, bag_path):
        """
        Given a full path to a directory, "bags" that data, writes the bag's information to file, and returns the new bag name.
        """
        # update self.now ONCE per bag
        time.sleep(1)
        self.now = datetime.datetime.now()
        self.create_bag_structure(bag_path)
        bag_path, bag_renamed = self.cleanse_bag_name(bag_path) # Remove special characters
        
        bag_path, identifier = self.format_bag_name(self.now,bag_path) # Add an identifier if needed
        
        # cleanse filenames in bag, save rename.csv metadata
        b_dict, needs_renaming = self.create_relative_bag_dict(bag_path,os.path.join(bag_path,"data","originals"),0)

        if needs_renaming or bag_renamed:
            renamed_files_list = self.cleanse_dict(os.path.join(bag_path,"data"),b_dict,0)
            self.write_rename_file(bag_path,renamed_files_list,bag_renamed)

        size, extensions, num_files = self.traverse_bag_contents(bag_path)
        
        if self.create_import_file:
            bag_name = os.path.basename(bag_path)

            self.import_row["Title"] = bag_name
            self.import_row["Content"] = bag_name
            self.import_row["Scope Content"] = bag_name
            self.import_row["Identifier"] = identifier

            conversion = 9.31323e-10
            file_size = size * conversion
            file_size_string = "%.2f" % file_size
            if file_size < 0.01:
                file_size_string = "0.01"

            self.import_row["Received Extent"] = file_size_string
            self.import_row["Processed Extent"] = file_size_string
            self.import_row["Extent"] = file_size_string
            
            extension_string = "Extensions include: "
            for item in extensions:
                if len(item) > 0:
                    extension_string = extension_string + item + "; "
            if len(extensions) == 0:
                extension_string = ""
            else:
                extension_string = extension_string[:len(extension_string)-2]

            self.import_row["Physical Description"] = extension_string

            # write the import template row
            new_row = []
            for item in self.import_header:
                new_row.append(self.import_row[item])
            self.import_writer.writerow(new_row)

        return os.path.basename(bag_path)

    def accession_file(self, file_path):
        """
        Given a full path to a file, creates a new "bag" to hold it, writes the bag's information to file, and returns the new bag name.
        """
        new_directory = os.path.splitext(file_path)[0]
        new_directory = new_directory.encode('cp850', errors='ignore')

        if os.path.exists(new_directory):
            i = 1
            while os.path.exists(new_directory + str(i)):
                i+=1
            new_directory = new_directory + str (i)
        os.mkdir(new_directory)
        os.rename(file_path,os.path.join(new_directory, os.path.basename(file_path)))
        file_path = new_directory

        return self.accession_bag(os.path.splitext(file_path)[0])

    def create_bag_structure(self, bag_path):
        files_in_bag = set(os.listdir(bag_path))
        
        # If directory bag/data is not present, create it.
        if not os.path.exists(os.path.join(bag_path,"data")):
            os.mkdir(os.path.join(bag_path,"data"))
        else:
            files_in_bag.remove("data")

        # If any of these directories are present: bag/dips, bag/meta and bag/originals - move them to bag/data.
        if os.path.exists(os.path.join(bag_path,"dips")):
            shutil.move(os.path.join(bag_path,"dips"),os.path.join(bag_path,"data"))
            files_in_bag.remove("dips")

        if os.path.exists(os.path.join(bag_path,"meta")):
            shutil.move(os.path.join(bag_path,"meta"),os.path.join(bag_path,"data"))
            files_in_bag.remove("meta")

        if os.path.exists(os.path.join(bag_path,"originals")):
            shutil.move(os.path.join(bag_path,"originals"),os.path.join(bag_path,"data"))
            files_in_bag.remove("originals")

        # If bag/data/originals is not present, create it. If bag/data/dips is not present, create it. If bag/data/meta is not present, create it.
        if not os.path.exists(os.path.join(bag_path,"data","originals")):
            os.mkdir(os.path.join(bag_path,"data","originals"))

        if not os.path.exists(os.path.join(bag_path,"data","dips")):
            os.mkdir(os.path.join(bag_path,"data","dips"))

        if not os.path.exists(os.path.join(bag_path,"data","meta")):
            os.mkdir(os.path.join(bag_path,"data","meta"))

        # All other files in bag, move to bag/data/originals.
        for f in files_in_bag:
            if not self.is_excluded(f):
                shutil.move(os.path.join(bag_path,f), os.path.join(bag_path,"data","originals"))

    def cleanse_bag_name(self, bag_path):
        """
        Remove special characters from bag names.
        """
        replacement_path = bag_path

        bag_name = os.path.basename(bag_path)
        replacement_name = bag_name

        for match in self.chars_to_remove.finditer(bag_name):
            replacement_name = replacement_name.replace(match.group(), "_")

        replacement_name = replacement_name.encode('cp850', errors='ignore')

        if replacement_name != bag_name:
            replacement_path = os.path.join(os.path.dirname(bag_path),replacement_name)
            os.rename(bag_path, replacement_path)

        bag_renamed = []
        if bag_name != replacement_name:
            bag_renamed.append("\\"+bag_name)
            bag_renamed.append("\\"+replacement_name)
        return (replacement_path, bag_renamed)

    def format_bag_name(self,now,bag_path):
        bag_name = os.path.basename(bag_path)

        # Again, format: yyyyddmm_hhmmss_originalDirTitle
        date_created = "%s%02d%02d_%02d%02d%02d" % (self.now.year, self.now.day, \
        self.now.month, self.now.hour, self.now.minute, self.now.second)

        if self.valid_bag_name_format.search(bag_name) == None:
            new_valid_bag_name = "%s_%s" % (date_created,bag_name)
            os.rename(bag_path,os.path.join(os.path.dirname(bag_path),new_valid_bag_name))
            bag_path = os.path.join(os.path.dirname(bag_path),new_valid_bag_name)

        return bag_path, date_created

    def create_relative_bag_dict(self, walk_path,path_to_originals,depth):
        """
        Given a bag with proper hierarchical structure, returns:
            -a dictionary with the proper relative paths for files.
            -a boolean value for whether anything needs to be renamed.
        """
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
    
    def cleanse_dict(self, path_to_data,bag_dict, depth):
        renamed_files_list = []
        for item in bag_dict:
            repl_str = item
            # Remove any chars if necessary
            repl_str = repl_str.encode('cp850', errors='ignore')
            for match in self.chars_to_remove.finditer(item):
                repl_str = repl_str.replace(match.group(), "_")
            # Rename the file if necessary, remove the previous key in place of the new one
            if repl_str != item:
                # If new name is in use, add an index
                if os.path.exists(os.path.join(path_to_data,repl_str)):
                    rename_index = 1
                    while os.path.exists(os.path.join(path_to_data,repl_str + "_" + str(rename_index))):
                        rename_index += 1
                    repl_str = repl_str + "_" + str(rename_index)
                # Rename the file
                os.rename(os.path.join(path_to_data,os.path.dirname(repl_str),os.path.basename(item)),os.path.join(path_to_data,repl_str))
                bag_dict[repl_str] = bag_dict[item]
                renamed_files_list.append(list((item.encode('cp850', errors='replace'), repl_str)))
                del bag_dict[item]
            if bag_dict[repl_str] != "":
                renamed_files_list = renamed_files_list + (self.cleanse_dict(path_to_data,bag_dict[repl_str], depth+1))
        return renamed_files_list

    def write_rename_file(self, bag_path, files_to_rename, bag_renamed):
        rename_file_path = os.path.join(bag_path,"data","meta","renames")
        out_file = ""
        append_to_old_file = True

        if os.path.exists(rename_file_path + ".csv"):
            out_file = open(rename_file_path + ".csv", "ab")
        else:
            out_file = open(rename_file_path + ".csv", "wb")
            append_to_old_file = False

        writer = csv.writer(out_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)

        if not append_to_old_file:
            writer.writerow(["Old_Name", "New_Name", "Date"])

        if len(bag_renamed) == 2:
            bag_renamed.append(str(self.now))
            writer.writerow(bag_renamed)

        for row in files_to_rename:
            row.append(str(self.now))
            writer.writerow(row)

        out_file.close()

    def traverse_bag_contents(self, bag_path, verbose=0):
        """
        Returns: size, extensions, num_files
        """
        total_size = 0
        file_types = set()
        num_files = 0
        # List of everything contained in the "originals" folder (the primary bag data we're concerned with)
        dirs_in_originals_folder = os.listdir(os.path.join(bag_path,"data","originals"))

        try:
            for root, dirs, files in os.walk(ast.literal_eval("u'" + (bag_path.replace("\\", "\\\\")) + "'"), topdown=False):
                if os.path.basename(root) == "originals":
                    num_files+=len(files) + len(dirs)
                elif os.path.basename(root) in dirs_in_originals_folder:
                    dirs_in_originals_folder = dirs_in_originals_folder + os.listdir(root)
                    # in case a directory after we've traversed originals has the same name as a directory in originals
                    dirs_in_originals_folder.remove(os.path.basename(root))
                    num_files+=len(files) + len(dirs)
                for names in files:
                    filepath = os.path.join(root,names)
                    if not self.is_excluded(names):
                        total_size += os.path.getsize(filepath)
                        ext_type = os.path.splitext(filepath)[1]
                        file_types.add(ext_type)
        except:
            import traceback
            # Print the stack traceback
            traceback.print_exc()
            sys.exit()

        return total_size, file_types, num_files

    def is_excluded(self, filename):
        if filename in self.excludes:
            return True
        for pattern in self.excludes_regex:
            if pattern.search(filename) != None:
                return True
        return False

def usage_message():
    print "\ndata_accessioner: Cleanses a directory of files/dirs and places them in properly-formatted bags using the bagit specifications.\
    \n\nUsage:\
            \n\tpython data_accessioner.py [options] <path>\
            \n\tpython data_accessioner.py -h | --help\
        \n\nOptions:\
            \n\t-h --help\tShow this screen.\
            \n\t-d --debug\tCreates a new copy of the original folder or file.\
        \n\nDependencies:\
            \n\taccession_settings.txt"

def main():
    accessioner = DataAccessioner('accession_settings.txt')

    if len(sys.argv) <= 1 or len(sys.argv) > 3 or sys.argv[1] == "-h" or sys.argv[1] == "--help":
        return usage_message()
    elif len(sys.argv) == 2:
        path_arg, import_file = sys.argv[1], True
    elif len(sys.argv) == 3:
        path_arg, import_file = sys.argv[2], True

    if os.path.exists(path_arg):
        if sys.argv[1] == "-d" or sys.argv[1] == "--debug":
            timestamp = "_%s%02d%02d_%02d%02d%02d" % (accessioner.now.year, accessioner.now.day, \
            accessioner.now.month, accessioner.now.hour, accessioner.now.minute, accessioner.now.second)
            shutil.copytree(path_arg, path_arg + timestamp)
            path_arg = path_arg + timestamp
        accessioner.accession_bags_in_dir(path_arg, import_file)

    else:
        print '\n<path> does not exist',
        usage_message()

main()