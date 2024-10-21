import os
import sys
import subprocess
import pyarrow as pq

print("[libraries.py.py] reading config file")
import configparser

#read the config file
def read_ini_file(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    
    variables = {}
    for section in config.sections():
        for key, value in config.items(section):
            variables[key] = value
    
    return variables

config_path = "config.ini"
variables = read_ini_file(config_path)

# Assign variables dynamically to the global namespace
for key, value in variables.items():
    globals()[key] = value

#change types of some
chunk_lim = int(chunk_lim)

# Print variables to verify
for key in variables.keys():
    print(f"{key} = {globals()[key]}")

def add_to_path(directory):
    if directory not in sys.path:
        sys.path.append(directory)

add_to_path(hash_ds_loc)
#load from hash_ds libraries
from libs import *
from chunk_df import *
