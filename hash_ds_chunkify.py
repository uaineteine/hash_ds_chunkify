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

#change types of some
chunk_lim = int(chunk_lim)

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
    for i, chunk in enumerate(range(0, len(df.index), chunk_size)):
        chunk_df = df.iloc[chunk:chunk + chunk_size]
        chunk_path = os.path.join(output_dir, f"chunk_{i}.parquet")
        chunk_df.to_parquet(chunk_path, index=False)
        chunk_list.append(chunk_path)
    return chunk_list

def read_and_clean_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Remove additional empty lines and trailing whitespace
    cleaned_content = "\n".join([line.rstrip() for line in content.splitlines() if line.strip()])
    
    return cleaned_content

def main():
    parser = argparse.ArgumentParser(description="Chunkify dataframe and hash specified columns")
    parser.add_argument('filepath', type=str, help='Path to the file to hash')
    parser.add_argument('columns', type=str, help='Comma-separated list of columns to hash')
    
    args = parser.parse_args()
    key = read_and_clean_file(keyfile_loc)
    filepath = args.filepath
    columns = args.columns

    print("[hash_ds_chunkfy.py] reading source file")
    df = read_file(filepath)

    print("[hash_ds_chunkfy.py] dividing df into chunks")
    chunks = chunkify_df(df, chunk_lim, chunks_dir)

    print("[hash_ds_chunkfy.py] hashing chunks...")
    os.chdir(hash_ds_loc)
    for chunk_path in chunks:
        subprocess.run(['python', "-m", 'hash_ds', chunk_path, columns, key, str(trunc_length)])

    print("[hash_ds_chunkfy.py] recombining...")
    # Combine the processed chunks
    combined_df = pd.concat([pd.read_parquet(chunk_path) for chunk_path in chunks], ignore_index=True)
    print(combined_df)

if __name__ == "__main__":
    main()
