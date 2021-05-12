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

## zips tables, producing ready for release zip files

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
    
        output_path = f"../../data/processed/tablecorpus/domain_tables_upload/{name}/"
        shutil.rmtree(output_path, ignore_errors=True)
        Path(f'{output_path}full/').mkdir(parents=True, exist_ok=True)
        Path(f'{output_path}statistics/').mkdir(parents=True, exist_ok=True)
        Path(f'{output_path}samples/').mkdir(parents=True, exist_ok=True)

        # path to folder which needs to be zipped 
        main_directory = f'../../data/processed/tablecorpus/domain_tables_sorted/{name}/'
        
        main_files = get_all_file_paths(main_directory)
        
        table_stats = []
        attribute_dist = []
        summary_stats = []
        
        for file in main_files:
            if 'column_statistics' in file:
                attribute_dist.append(file)
            elif 'September2020_statistics' in file:
                table_stats.append(file)
            elif 'summary_statistics' in file:
                summary_stats.append(file)
            else:
                print('cannot happen!')
                
        with ZipFile(f'{output_path}statistics/{name}_statistics.zip','w') as zip: 
                # writing each file one by one 
                for file in attribute_dist:
                    filename = os.path.basename(file)
                    if 'gt2' in filename:
                        filename = filename.replace('gt2', 'minimum3')
                    zip.write(file, arcname=f'column_statistics/{filename}') 
                    
                for file in table_stats:
                    filename = os.path.basename(file)
                    if 'gt2' in filename:
                        filename = filename.replace('gt2', 'minimum3')
                    zip.write(file, arcname=f'table_statistics/{filename}') 
                    
                for file in summary_stats:
                    filename = os.path.basename(file)
                    if 'gt2' in filename:
                        filename = filename.replace('gt2', 'minimum3')
                    zip.write(file, arcname=f'summary_statistics/{filename}') 

                
        
        for folder in ['top100', 'gt2', 'rest']:
            directory = f'{main_directory}{folder}/'

            # calling function to get all file paths in the directory 
            file_paths = get_all_file_paths(directory) 
            if len(file_paths) == 0:
                continue
            
            if folder == 'gt2':
                folder = 'minimum3'
            # writing files to a zipfile 
            with ZipFile(f'{output_path}full/{name}_{folder}.zip','w') as zip: 
                # writing each file one by one 
                for file in file_paths: 
                    zip.write(file, arcname=os.path.basename(file))    
  
        samples_directory = f'../../data/processed/tablecorpus/domain_tables_samples/{name}/top100/'
        
        sample_files = get_all_file_paths(samples_directory) 
        csv_samples = []
        json_samples = []
        
        for file in sample_files:
            if 'csvsample_' in file:
                csv_samples.append(file)
            elif 'jsonsample' in file:
                json_samples.append(file)
            else:
                print('cannot happen!')
                
        csv_sample = random.sample(csv_samples, 1)[0]
        json_sample = random.sample(json_samples, 1)[0]
        
        shutil.copy(csv_sample, f'{output_path}samples/{name}_csvsample.csv')
        shutil.copy(json_sample, f'{output_path}samples/{name}_jsonsample.json')