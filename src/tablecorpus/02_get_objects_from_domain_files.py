import pandas as pd
import numpy as np
np.random.seed(42)
import random
random.seed(42)

import pickle
import gzip
import glob
import os
import json

from tqdm import tqdm
from pathlib import Path
import shutil
from urllib.parse import urlparse

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from pdb import set_trace

## Parses over domain-wise nquad files and extracts all objects of class X by URL

# Set the amount of workers to pass to ProcessPoolExecutor or ThreadPoolExecutor
MAX_WORKERS = None

def get_objects_from_domain(arg_tuple):
    
    domain_path, domain_name, name, main_class, output_path = arg_tuple
    domain_dict= {}
    
    with gzip.open(domain_path, 'rt', encoding='utf-8') as f:
        for i, line in enumerate(f):
            line = line.rstrip()
            split = line.split()
            so_class = ' '.join(split[2:-2])
            so_class = so_class[1:-1]
            so_class = so_class.replace('https', 'http')
            so_class = so_class.lower()
            url = split[-2]
            url = url[1:-1]

            if so_class == main_class:
                node_id = split[0]
                
                if url in domain_dict.keys():
                    domain_dict[url].add(node_id)
                else:
                    domain_dict[url] = set([node_id])
                    
    with gzip.open(f"{output_path}{domain_name}.pkl.gz","wb") as f:
        pickle.dump(domain_dict, f)


with open('../../schemaorg_classes.json', 'r') as f:
    class_dict = json.load(f)

classes = [k for k in class_dict.keys()]

for name in tqdm(classes):

    for k in class_dict.keys():
        if k == name:
            main_class = class_dict[k].lower()

    domains = glob.glob(f'../../data/raw/tablecorpus/by-domain/{name}/*.gz')
    print(f'Class: {name} contains {len(domains)} domains.\n')
    
    output_path = f'../../data/interim/tablecorpus/domain_objects/{name}/'
    shutil.rmtree(output_path, ignore_errors=True)
    Path(output_path).mkdir(parents=True, exist_ok=True)
    
    arguments = ((v, os.path.basename(v)[:-3], name, main_class, output_path) for v in list(domains))
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(get_objects_from_domain, arguments)