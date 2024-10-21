import os
import pandas as pd
import pyarrow
from hash_ds import read_file
print("[hash_ds_chunkify::chunk_df.py] defining functions")

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

def combine_list_ds(chunks):
    # Combine the processed chunks
    combined_df = pd.concat([read_file(chunk_path) for chunk_path in chunks], ignore_index=True)
    return combined_df

def delete_chunks(chunks):
    for filepath in chunks:
        try:
            os.remove(filepath)
        except Exception as e:
            print(f'Error deleting file {filepath}: {e}')
    print("[hash_ds_chunkify::chunk_df.py] deleted all temporary chunks")

def remove_directory(directory_path):
    try:
        os.rmdir(directory_path)
        print(f'Removed directory: {directory_path}')
    except Exception as e:
        print(f'Error removing directory {directory_path}: {e}'
    print("[hash_ds_chunkify::chunk_df.py] removed directory for chunks")
