import os, glob
import sys
import argparse
import pandas as pd

from azureml.core import Run
from azureml.core.run import Run

import cfgrib
import xarray as xr

def getRuntimeArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-path', type=str)
    args = parser.parse_args()
    return args


def main():
    args = getRuntimeArgs()
    run = Run.get_context()

    file_name = '_D2D05150000051703001'
    file_path = os.path.join(args.data_path, file_name)

    print(f'Data Path: {args.data_path}')
    print(f'Files in data path: {glob.glob(args.data_path)}')
    print(f'Filename: {file_name}')
    print(f'Full file path: {file_path}')

    ds = cfgrib.open_datasets(file_path, backend_kwargs={'read_keys': ['pv'], 'indexpath': ''})

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

    # TODO: Write more meaningful code here

if __name__ == "__main__":
    main()