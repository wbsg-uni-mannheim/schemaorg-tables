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

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from pdb import set_trace

## Builds the early version of the final table from the extracted attributes in step 03

# Set the amount of workers to pass to ProcessPoolExecutor or ThreadPoolExecutor
MAX_WORKERS = None

def build_tables(arg_tuple):
    
    domain_path, domain_name, name, main_class, output_path = arg_tuple
    
    attribute_stats_dict = {}
    count_objects = 0
    thresholds = [25]
    
    with gzip.open(domain_path, "rb") as f:
        domain_objects_dict = pickle.load(f)
    
    if len(domain_objects_dict) < 1:
        return
    
    count_objects = len(domain_objects_dict)
    
    for obj, attribute_dict in domain_objects_dict.items():
        for attribute, values in attribute_dict.items():
            if attribute not in attribute_stats_dict.keys():
                attribute_stats_dict[attribute] = 1
            else:
                if len(attribute_dict[attribute]) != 0:
                    attribute_stats_dict[attribute] += 1
                
    for threshold in thresholds:
        
        Path(f'{output_path}{threshold}/').mkdir(parents=True, exist_ok=True)
        
        attribute_stats_cut_dict = {k:v for k, v in attribute_stats_dict.items() if (v/count_objects)*100 > threshold}
        table_dict = {k:list() for k in attribute_stats_cut_dict.keys()}
        table_dict['node_id'] = list()
        table_dict['page_url'] = list()
        
        for obj, attribute_dict in domain_objects_dict.items():
            
            for attribute, values in table_dict.items():
                if attribute == 'node_id':
                    values.append(obj[0])
                elif attribute == 'page_url':
                    values.append(obj[1])
                elif attribute in attribute_dict.keys():
                    final_attribute = []
                    attr_value_set = attribute_dict[attribute]
                    if len(attr_value_set) != 0:
                        if len(attr_value_set) == 1 and type(list(attr_value_set)[0]) == str:
                            values.append(list(attr_value_set)[0])
                        else:
                            for sub_set in attr_value_set:
                                if type(sub_set) == str:
                                    final_attribute.append(sub_set)
                                else:
                                    sub_dict = {}
                                    for sub_sub_set in sub_set:
                                        if len(sub_sub_set) == 1:
                                            tup = list(sub_sub_set)[0]
                                            if tup[0] in sub_dict:
                                                if type(sub_dict[tup[0]]) == list:
                                                    sub_dict[tup[0]].append(tup[1])
                                                else:
                                                    sub_dict[tup[0]] = [sub_dict[tup[0]]]
                                                    sub_dict[tup[0]].append(tup[1])
                                            else:
                                                sub_dict[tup[0]] = tup[1]
                                        else:
                                            for sub_sub_sub_set in sub_sub_set:
                                                tup = sub_sub_sub_set
                                                if tup[0] in sub_dict:
                                                    if type(sub_dict[tup[0]]) == list:
                                                        sub_dict[tup[0]].append(tup[1])
                                                    else:
                                                        sub_dict[tup[0]] = [sub_dict[tup[0]]]
                                                        sub_dict[tup[0]].append(tup[1])
                                                else:
                                                    sub_dict[tup[0]] = tup[1]
                                    final_attribute.append(sub_dict)
                            if len(final_attribute) == 1:
                                values.append(final_attribute[0])
                            else:
                                values.append(final_attribute)
                    else:
                        values.append(None)                
                else:
                    values.append(None)
                
        table_df = pd.DataFrame(table_dict)
        table_df.to_pickle(f'{output_path}{threshold}/{domain_name}.pkl.gz')


with open('../../schemaorg_classes.json', 'r') as f:
    class_dict = json.load(f)

classes = [k for k in class_dict.keys()]

for name in tqdm(classes):
    for k in class_dict.keys():
        if k == name:
            main_class = class_dict[k].lower()
    
    output_path = f"../../data/interim/tablecorpus/domain_tables/{name}/"
    shutil.rmtree(output_path, ignore_errors=True)
    Path(output_path).mkdir(parents=True, exist_ok=True)
    
    domains = glob.glob(f'../../data/interim/tablecorpus/domain_objects_extracted/delist-dedup/{name}/*.pkl.gz')
        
    arguments = ((v, os.path.basename(v)[:-7], name, main_class, output_path) for v in list(domains))
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(build_tables, arguments)