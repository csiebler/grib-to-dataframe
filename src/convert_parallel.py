import os
import glob
import json
import argparse
import pandas as pd
from azureml.core import Run

import cfgrib
import xarray as xr

current_run = None
data_output_path = None

def init():
    print('Started data conversion by running init()')

    parser = argparse.ArgumentParser('convert_parallel')

    parser.add_argument('--data_output_path',
                        type=str,
                        dest='data_output_path',
                        help='Converted data output directory')
    args, _ = parser.parse_known_args()

    global current_run
    global data_output_path
    current_run = Run.get_context()
    data_output_path = args.data_output_path

    print(f'Arguments: {args}')
    print(f'Output data path: {data_output_path}')

def run(file_list):
    print(f'File list for minibatch: {file_list}')
    output_list = []
    try:
        for filepath in file_list:
            print("Starting converting:", filepath)

            convert_and_write_df(filepath)

            result = {'filepath': filepath, 'status': 'converted'}
            output_list.append(result)
            print(result)
    except Exception as e:
        error = str(e)
        print('Error:', error)
    
    return output_list

def convert_and_write_df(filepath):

    filename = os.path.basename(filepath)
    outfile = os.path.join(data_output_path, "converted" + filename + ".parquet.gzip")

    print(f'Processing file {filename}')
    ds = cfgrib.open_datasets(filepath, backend_kwargs={'read_keys': ['pv'], 'indexpath': ''})

    for i in range(len(ds)):
        outfile = os.path.join(data_output_path, "converted" + filename + str(i) + ".parquet.gzip")
        pdf = ds[i].to_dataframe()
        pdf.reset_index(inplace=True)
        pdf = pdf.drop(columns='step')
        print(f'Writing results to {outfile}')
        pdf.to_parquet(outfile, compression='gzip')

def test():
    # Simulate AML ingesting paths to the data files
    files = ['../sample-data/_D2D05150000051703001']
    result = run(files)
    print(result)

if __name__ == "__main__":
    test()
