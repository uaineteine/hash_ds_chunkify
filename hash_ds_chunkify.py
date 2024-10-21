print("[hash_ds_chunkify.py] reading config file")
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

# Print variables to verify
for key in variables.keys():
    print(f"{key} = {globals()[key]}")

print("[hash_ds_chunkify.py] importing libraries...")
import os
import sys

def add_to_path(directory):
    if directory not in sys.path:
        sys.path.append(directory)

add_to_path(hash_ds_loc)
from libs import *
from hash_ds import read_file
import subprocess
import pyarrow as pq

print("[hash_ds_chunkify.py] defining functions...")
def chunkify_df(df, chunk_size, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    chunk_list = []
    for i, chunk in enumerate(range(0, len(df), chunk_size)):
        chunk_df = df.iloc[chunk:chunk + chunk_size]
        chunk_path = os.path.join(output_dir, f"chunk_{i}.csv")
        chunk_df.to_parquet(chunk_path, index=False)
        chunk_list.append(chunk_path)
    return chunk_list

def main():
    parser = argparse.ArgumentParser(description="Chunkify dataframe and hash specified columns")
    parser.add_argument('filepath', type=str, help='Path to the file to hash')
    parser.add_argument('columns', type=str, help='Comma-separated list of columns to hash')
    parser.add_argument('key', type=str, help='Key for hashing')
    parser.add_argument('length', type=int, help='Length of hash to truncate to')
    parser.add_argument('chunks_dir', type=str, help='Temporary path for the chunks')

    args = parser.parse_args()
    filepath = args.filepath
    columns = args.columns
    key = args.key
    length = args.length
    chunks_dir = args.chunks_dir

    print("[hash_ds_chunkfy.py] reading source file")
    df = read_file(filepath)

    print("[hash_ds_chunkfy.py] dividing df into chunks")
    chunks = chunkify_df(df, chunk_lim, chunks_dir)

    print("[hash_ds_chunkfy.py] hashing chunks...")
    for chunk_path in chunks:
        subprocess.run(['python', 'hash_ds.py', chunk_path, columns, key, str(length)])

    print("[hash_ds_chunkfy.py] recombining...")
    # Combine the processed chunks
    combined_df = pd.concat([pd.read_csv(chunk_path) for chunk_path in chunks])
    print(combined_df)

if __name__ == "__main__":
    main()
