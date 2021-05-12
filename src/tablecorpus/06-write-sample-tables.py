import pandas as pd
import numpy as np
np.random.seed(42)
import random
random.seed(42)

import pickle
import gzip
import glob
import os
import re
from copy import deepcopy
import json

from tqdm import tqdm
from pathlib import Path
import shutil
from urllib.parse import urlparse

from random import sample

from pdb import set_trace

## generates sample tables

with open('../../schemaorg_classes.json', 'r') as f:
    class_dict = json.load(f)

classes = [k for k in class_dict.keys()]


for name in tqdm(classes):
    
    output_path = f"../../data/processed/tablecorpus/domain_tables_samples/{name}/"
    shutil.rmtree(output_path, ignore_errors=True)
    Path(output_path).mkdir(parents=True, exist_ok=True)
    
    thresholds = [25]
    threshold_dict = {}

    for threshold in thresholds:

        tables_top100 = glob.glob(f'../../data/processed/tablecorpus/domain_tables_sorted/{name}/top100/*.json.gz')
        tables_gt2 = glob.glob(f'../../data/processed/tablecorpus/domain_tables_sorted/{name}/gt2/*.json.gz')
        tables_rest = glob.glob(f'../../data/processed/tablecorpus/domain_tables_sorted/{name}/rest/*.json.gz')
        
        sample_list = {'top100':tables_top100,
                       'gt2':tables_gt2,
                       'rest':tables_rest}
        
        for group, tables in sample_list.items():
            
            try:
                random_selection = sample(tables,100)
            except ValueError:
                random_selection = tables
            
            subpath = f'{output_path}{group}/'
            Path(f'{subpath}full_tables_xlsx/').mkdir(parents=True, exist_ok=True)
            
            for table_path in random_selection:
                
                domain_name = os.path.basename(table_path)
                domain_name = domain_name[:-8]
                
                df = pd.read_json(table_path, lines=True)
                df.to_excel(f'{subpath}full_tables_xlsx/{domain_name}.xlsx', index=False)
                df.iloc[:25].to_json(f'{subpath}jsonsample_{domain_name}.json', orient='records', lines=True, force_ascii=False)
                df.iloc[:25].to_csv(f'{subpath}csvsample_{domain_name}.csv', index=False)