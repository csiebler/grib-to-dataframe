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

    print(f'Arguments: {args}')
    print(f'Output data path: {args.data_output_path}')

    global current_run
    global data_output_path
    current_run = Run.get_context()
    data_output_path = args.data_output_path

def run(file_list):
    print(f'File list for minibatch: {file_list}')
    try:
        output_list = []
        for filepath in file_list:
            print("Starting converting:", filepath)

            convert_and_write_df(filepath)

            result = {'filepath': filepath, 'status': 'converted'}
            output_list.append(result)

            print("Converted:", filepath)

        return output_list
    except Exception as e:
        error = str(e)
        return error

def convert_and_write_df(filepath):

    filename = os.path.basename(filepath)
    outfile = os.path.join(data_output_path, "converted" + filename + ".parquet.gzip")

    print(f'Processing file {filename}')
    ds = cfgrib.open_datasets(filepath, backend_kwargs={'read_keys': ['pv'], 'indexpath': ''})

    # TODO: Add more useful code here

    x = ds[0].t.attrs['GRIB_pv']
    nl = len(x)
    
    if nl == 184: #91 vertical levels
        a = dict(zip(range(92), list(x[0: 92])))
        b = dict(zip(range(92), list(x[92: 184])))
    elif nl == 276: #137 vertical levels
        a = dict(zip(range(138), list(x[0: 138])))
        b = dict(zip(range(138), list(x[138: 276])))
    else:
        raise Exception("Cannot retrieve a,b parameters for vertical levels!")
    
    ab = pd.DataFrame(index = a.keys(), columns = ['A'], data = a.values())
    ab['B'] = b.values()

    print(f'Writing results to {outfile}')
    ab.to_parquet(outfile, compression='gzip')

def test():
    # Simulate AML ingesting paths to the data files
    files = ['../../sample-data/_D2D05150000051703001']
    result = run(files)
    print(result)

if __name__ == "__main__":
    test()
