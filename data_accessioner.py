''' 
data_accessioner.py
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

class DataAccessioner:
    def __init__(self,settings_file):
        self.excludes = []
        self.storage_location_name = ""
        self.initialize_accession_settings(settings_file)

        # Regular expressions for file name cleansing
        # Bag format: yyyyddmm_hhmmss_originalDirTitle
        self.valid_bag_name_format = re.compile("^[0-9]{8}_[0-9]{6}_.*")
        self.chars_to_remove = re.compile("[:/'\+\=,\-\!\@\#\$\%\^\&\*\(\)\]\[ \t]") # characters to remove from files

        # Global dictionaries/lists for use in various methods
        self.original_file_names = {} # entries will be of the form {new_name:old_name}
        self.original_dir_names = {} # entries will be of the form {new_name:old_name}

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
        now = datetime.datetime.now()
        self.import_row["Month"] = now.month
        self.import_row["Day"] = now.day
        self.import_row["Year"] = now.year
        self.import_row["Extent Unit"] = "Gigabytes"
        self.import_row["Location"] = self.storage_location_name
        self.import_row["ExtentUnit"] = "Items"

    def initialize_accession_settings(self,settings_file):
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
                elif parse_state == "excludes":
                    if line != "":
                        self.excludes.append(line)
                    else:
                        parse_state = "none"

    def initialize_import_file(self, top_dir):
        now = datetime.datetime.now()
        file_name = "ImportTemplate_%s%02d%02d" % (now.year, now.month, now.day)
        i = ""
        if os.path.exists(os.path.join(top_dir,file_name + '.csv')):
            file_name = file_name + "_"
            i = 1
            while os.path.exists(os.path.join(top_dir,file_name + str(i) + '.csv')):
                i+=1
        self.import_file = open(os.path.join(top_dir,file_name + str(i) + '.csv'),'wb')
        self.import_writer = csv.writer(self.import_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        self.import_writer.writerow(self.import_header)

    def accession_bags_in_dir(self,top_dir):
        self.initialize_import_file(top_dir)
        print "accessioning...\n-----"
        path_list = os.listdir(ast.literal_eval("u'" + (top_dir.replace("\\", "\\\\")) + "'"))
        for bag in path_list:
            if bag not in self.excludes:
                print "current bag:", bag, "\n"
                full_bag_path = os.path.join(top_dir,bag)
                if os.path.isdir(full_bag_path):
                    bag = self.accession_bag(full_bag_path)
                else:
                    bag = self.accession_file(full_bag_path)
                print "accessioning complete for", bag, "\n-----"
        print "done"

    def accession_bag(self, bag_path):
        """
        Given a full path to a directory, "bags" that data, writes the bag's information to file, and returns the new bag name.
        """
        self.create_bag_structure(bag_path)
        bag_path = self.cleanse_bag_name(bag_path) # Remove special characters
        
        bag_path = self.format_bag_name(bag_path) # Add an identifier if needed

        bag_name = os.path.basename(bag_path)

        self.import_row["Title"] = bag_name
        self.import_row["Content"] = bag_name
        self.import_row["Scope Content"] = bag_name

        identifier_string = bag_name[:15].replace("_","/") 
        
        # CHECK EVERYTHING PAST HERE
        hashes, size, extensions, num_files = self.traverse_bag_contents(bag_path)
        
        self.import_row["Comments"] = "md5 hash: " + hashes
        self.import_row["Extent"] = str(num_files)

        conversion = 9.31323e-10
        file_size = size * conversion
        file_size_string = "%.2f" % file_size
        self.import_row["Received Extent"] = file_size_string
        self.import_row["Processed Extent"] = file_size_string
        
        extension_string = "Extensions include: "
        for item in extensions:
            if len(item) > 0:
                extension_string = extension_string + item + "; "
        if len(extensions) == 0:
            extension_string = ""
        else:
            extension_string = extension_string[:len(extension_string)-2]

        self.import_row["Physical Description"] = extension_string

        # write the rename file
        if not self.dict_is_empty(self.original_file_names) or not self.dict_is_empty(self.original_dir_names):
            self.write_rename_file(bag_path)

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
        return "FILE"

    def format_bag_name(self,bag_path):
        bag_name = os.path.basename(bag_path)
        if self.valid_bag_name.search(bag_name) == None:
            creation_time = datetime.datetime.fromtimestamp(os.path.getctime(bag_path))

            # Again, format: yyyyddmm_hhmmss_originalDirTitle
            date_created = "%s%02d%02d_%02d%02d%02d" % (creation_time.year, creation_time.day, \
            creation_time.month, creation_time.hour, creation_time.minute, creation_time.second)
            new_valid_bag_name = "%s_%s" % (date_created,bag_name)
            self.update_original_dir_names(bag_name, new_valid_bag_name)
            os.rename(bag_path,os.path.join(os.path.dirname(bag_path),new_valid_bag_name))
            bag_path = os.path.join(os.path.dirname(bag_path),new_valid_bag_name)

        return bag_path

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
            self.update_original_dir_names(bag_name,replacement_name)
            replacement_path = os.path.join(os.path.dirname(bag_path),replacement_name)
            os.rename(bag_path, replacement_path)

        return replacement_path

    def cleanse_dir_name(self, dir_path, bag_path):
        dir_name = os.path.basename(dir_path)
        replacement_name = dir_name
        
        for match in self.chars_to_remove.finditer(dir_name):
            replacement_name = replacement_name.replace(match.group(),"_")
        replacement_name = replacement_name.encode('cp850', errors='ignore')

        if replacement_name != file_name:
            relative_path = os.path.dirname(relpath(dir_path,bag_path))

    #       if replacement_name != file_name:
    #           #TODO: Check if this is okay?
    #           relative_path = os.path.relpath(file_dir_name,root_path)
    #           self.original_dir_names[os.path.join(relative_path, replacement_name)] = os.path.join(relative_path,file_name)
    #           replacement_path = os.path.join(file_dir_name,replacement_name)
    #           os.rename(file_path,replacement_path)
    #           file_path = replacement_path

    #       return file_path

    def update_original_dir_names(self, old_bag_name,new_bag_name):
        if old_bag_name in self.original_dir_names:
            original_name = self.original_dir_names[old_bag_name]
            self.original_dir_names[new_bag_name] = original_name
            del self.original_dir_names[old_bag_name]
        else:
            self.original_dir_names[new_bag_name] = old_bag_name

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
            if not f in self.excludes:
                shutil.move(os.path.join(bag_path,f), os.path.join(bag_path,"data","originals"))

    def write_rename_file(self, bag_path):
        rename_file_path = os.path.join(bag_path,"data","meta","renames")
        out_file = ""
        append_to_old_file = True
        
        if os.path.exists(rename_file_path + ".csv"):
            out_file = open(rename_file_path + ".csv", "a")
        else:
            out_file = open(rename_file_path + ".csv", "wb")
            append_to_old_file = False

        writer = csv.writer(out_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)

        if not append_to_old_file:
            writer.writerow(["New_Name", "Old_Name", "Date"])

        for entry in self.original_dir_names.keys():
            row_to_write = [entry,self.original_dir_names[entry].encode('utf-8'), str(datetime.datetime.now())]
            writer.writerow(row_to_write)

        for entry in self.original_file_names.keys():
            row_to_write = [entry,self.original_file_names[entry].encode('utf-8'),str(datetime.datetime.now())]
            writer.writerow(row_to_write)

        out_file.close()
        self.original_file_names.clear()
        self.original_dir_names.clear()

    def dict_is_empty(self, my_dict):
        for entry in my_dict.keys():
            return False
        return True

    def traverse_bag_contents(self, bag_path, verbose=0):
        """
        Returns: hashes, size, extensions, num_files
        """
        md5Hash = hashlib.md5() # Will be the hash returned for the directory.

        total_size = 0
        file_types = set()
        num_files = 0
        # List of everything contained in the "originals" folder (the primary bag data we're concerned with)
        dirs_in_originals_folder = os.listdir(os.path.join(bag_path,"originals"))

        try:
            for root, dirs, files in os.walk(ast.literal_eval("u'" + (bag_path.replace("\\", "\\\\")) + "'"), topdown=False):
                if self.chars_to_remove.search(os.path.basename(root)) != None:
                    root = self.cleanse_dir_name(root,bag_path)
                if os.path.basename(root) == "originals":
                    num_files+=len(files) + len(dirs)
                elif os.path.basename(root) in dirs_in_originals_folder:
                    dirs_in_originals_folder = dirs_in_originals_folder + os.listdir(root)
                    # in case a directory after we've traversed originals has the same name as a directory in originals
                    dirs_in_originals_folder.remove(os.path.basename(root))
                    num_files+=len(files) + len(dirs)
                for names in files:
                    if verbose == 1:
                        print 'Hashing', names
                    filepath = self.cleanse_file_name(root,names)
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
                    if not names in self.excludes:
                        total_size += os.path.getsize(filepath)
                        ext_type = os.path.splitext(filepath)[1]
                        file_types.add(ext_type)

        except:
            import traceback
            # Print the stack traceback
            traceback.print_exc()
            sys.exit()

        return md5Hash.hexdigest(), total_size, file_types, num_files


def main():
    accessioner = DataAccessioner('accession_settings.txt')
    accessioner.accession_bags_in_dir(sys.argv[1])

main()