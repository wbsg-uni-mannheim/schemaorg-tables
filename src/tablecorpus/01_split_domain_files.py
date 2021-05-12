import pandas as pd
import numpy as np
np.random.seed(42)
import random
random.seed(42)

import pickle
import gzip
import json

from tqdm import tqdm
from tldextract import extract
from pathlib import Path
import shutil
from urllib.parse import urlparse
import gc

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from pdb import set_trace

## Reads nquads and splits them into domain-wise files for easier processing in later pipeline stages

def write_file(kv):
    with gzip.open(f'{kv[2]}{kv[0]}.gz', 'at', encoding='utf-8') as dump_file:
        dump_file.write(''.join(kv[1]))

def domain_parse(name):
    
    input_path = '../../data/raw/tablecorpus/'
    
    output_path = f'../../data/raw/tablecorpus/by-domain/{name}/'
    shutil.rmtree(output_path, ignore_errors=True)
    Path(output_path).mkdir(parents=True, exist_ok=True)
    
    for folder in ['md/', 'json/']:
                
        line_dump_dict = None
        line_dump_dict = {}
        
        file = f'{input_path}{folder}schema_{name}.gz'
        with gzip.open(file, 'rt', encoding='utf-8') as f:
            num_lines = sum(1 for line in f)    
        with gzip.open(file, 'rt', encoding='utf-8') as f:
            for i, line in enumerate(tqdm(f, total=num_lines, desc=name)):
                
                line = line.rstrip()
                split = line.split()
                
                try:
                    url = split[-2]
                except IndexError:
                    set_trace()
                    
                url = url[1:-1]
                
                tsd, td, tsu = extract(url)

                if tsu == '':
                    domain = td
                else:
                    domain = td + '.' + tsu
                
                if domain in line_dump_dict.keys():
                    line_dump_dict[domain].append(f'{line}\n')
                else:
                    line_dump_dict[domain] = [f'{line}\n']
                    
                if i % 100000000 == 0:
                    arguments = ((k, v, output_path) for k, v in line_dump_dict.items())
                    with ThreadPoolExecutor() as executor:
                        executor.map(write_file, arguments)
                    arguments = None
                    line_dump_dict = None
                    gc.collect()
                    line_dump_dict = {}
            
            arguments = ((k, v, output_path) for k, v in line_dump_dict.items())
            with ThreadPoolExecutor() as executor:
                executor.map(write_file, arguments)
            arguments = None
            line_dump_dict = None
            gc.collect()
            line_dump_dict = {}
             

with open('../../schemaorg_classes.json', 'r') as f:
    class_dict = json.load(f)

classes = [k for k in class_dict.keys()]
for name in classes:
    domain_parse(name)