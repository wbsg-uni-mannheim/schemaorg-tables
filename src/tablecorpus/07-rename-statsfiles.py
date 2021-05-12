import pandas as pd
import numpy as np
np.random.seed(1)
import random
random.seed(1)

import pickle
import gzip
import glob
import os
from zipfile import ZipFile
import re
from copy import deepcopy
from statistics import quantiles, median
import json

from tqdm import tqdm
from pathlib import Path
import shutil
from urllib.parse import urlparse

import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from pdb import set_trace

## renames some properties in statsfiles to ensure consistent naming of concepts

def get_all_file_paths(directory): 
  
    # initializing empty file paths list 
    file_paths = [] 
  
    # crawling through directory and subdirectories 
    for root, directories, files in os.walk(directory): 
        for filename in files:
            # join the two strings in order to form the full filepath. 
            filepath = os.path.join(root, filename) 
            file_paths.append(filepath) 
        break
    # returning all file paths
    return file_paths         
    
    
if __name__ == "__main__":
    
    with open('../../schemaorg_classes.json', 'r') as f:
        class_dict = json.load(f)
    
    classes = [k for k in class_dict.keys()]

    for name in tqdm(classes):
        for k in class_dict.keys():
            if k == name:
                main_class = class_dict[k]

        # path to folder which needs to be zipped 
        main_directory = f'../../data/processed/tablecorpus/domain_tables_sorted/{name}/'
        
        main_files = get_all_file_paths(main_directory)
        
        table_stats = []
        summary_stats = []
        
        for file in main_files:
            
            if 'September2020_statistics' in file:
                table_stats.append(file)
            elif 'summary_statistics' in file:
                summary_stats.append(file)
            elif 'column_statistics' in file:
                continue
            else:
                print('cannot happen!')
            
        for file in table_stats:
            df = pd.read_csv(file)
            df = df.rename(columns={
                'domain':'host',
                'attribute_count':'column_count',
                'attribute_name_and_density': 'column_name_and_density'
            })
            df.to_csv(file, index=False)

        for file in summary_stats:
            contents = []
            with open(file, 'r') as f:
                for line in f:
                    contents.append(line)
            
            for i, line in enumerate(deepcopy(contents)):
                if i == 1:
                    contents[i] = line.replace('domains', 'tables')
                if i == 2:
                    contents[i] = line.replace('objects', 'rows')
                if i == 3:
                    contents[i] = line.replace('objects', 'rows')
            
            with open(file, 'w') as f:
                for line in contents:
                    f.write(line)