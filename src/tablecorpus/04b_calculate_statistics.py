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
import statistics
import seaborn as sns
import matplotlib.pyplot as plt
sns.set_theme()

import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from pdb import set_trace

## Calculates some statistics which are necessary for sorting by global attribute count in step 05

def process_domain(domain_path):
    domain_name = os.path.basename(domain_path)
    domain_name = domain_name[:-7]
    
    with gzip.open(domain_path, "rb") as f:
        domain_objects_dict = pickle.load(f)

    assert len(domain_objects_dict) != 0
    
    if len(domain_objects_dict) > 2:
        domain_tup = (domain_name, len(domain_objects_dict), True)
    else:
        domain_tup = (domain_name, len(domain_objects_dict), False)
        
    return domain_tup

def calc_stats(domains, name, main_class, output_path, step):
    
    object_per_domain_dict = {}
    number_domains = 0
    objects_overall = 0
    
    gt2_object_per_domain_dict = {}
    gt2_number_domains = 0
    gt2_objects_overall = 0
    
    arguments = list(domains)
    results = []
    
    with ThreadPoolExecutor() as executor:
        futures = []
        for domain in arguments:
            futures.append(executor.submit(process_domain, domain))
        
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
            
    for result in results:
        is_gt2 = result[2]
        if is_gt2:
            gt2_object_per_domain_dict[result[0]] = result[1]
        object_per_domain_dict[result[0]] = result[1]
    
    objects_overall = sum([v for k,v in object_per_domain_dict.items()])
    gt2_objects_overall = sum([v for k,v in gt2_object_per_domain_dict.items()])
    
    number_domains= len(object_per_domain_dict)
    gt2_number_domains= len(gt2_object_per_domain_dict)
    
    object_per_domain_sorted = [(k,v) for k, v in sorted(object_per_domain_dict.items(), key=lambda item: item[1], reverse=True)]
    top100_domains = object_per_domain_sorted[:100]
    
    low = []
    rest = []
    
    for tup in object_per_domain_sorted[100:]:
        if tup[1] > 2:
            rest.append(tup)
        else:
            low.append(tup)
    
    top100_domains_dict = {k:v for (k, v) in top100_domains}
    top100_domains_series = pd.Series(top100_domains_dict)
    
    low_domains_dict = {k:v for (k, v) in low}
    low_domains_series = pd.Series(low_domains_dict)
    
    rest_domains_dict = {k:v for (k, v) in rest}
    rest_domains_series = pd.Series(rest_domains_dict)
    
    
    
    sorted_object_per_domain_list = [v for (k,v) in object_per_domain_sorted]
    
    gt2_sorted_object_per_domain_list = sorted([v for k, v in gt2_object_per_domain_dict.items()], reverse=True)
 
    median_objects_per_domain = statistics.median(sorted_object_per_domain_list)
    
    gt2_median_objects_per_domain = statistics.median(gt2_sorted_object_per_domain_list)
    
    with open(f'{output_path}{name}_{step}_statistics.txt', 'w') as f:
        f.write(f'Number of domains\t{number_domains}\nNumber of objects\t{objects_overall}\nMedian objects per domain\t{median_objects_per_domain}\n')
            
    with open(f'{output_path}gt2_{name}_{step}_statistics.txt', 'w') as f:
        f.write(f'Number of domains\t{gt2_number_domains}\nNumber of objects\t{gt2_objects_overall}\nMedian objects per domain\t{gt2_median_objects_per_domain}\n')
            
    sns.lineplot(x=list(range(0, len(sorted_object_per_domain_list))), y=sorted_object_per_domain_list)    
    plt.xlabel("Domain ID")
    plt.ylabel("# of objects")
    plt.title("Objects per domain distribution")
    plt.savefig(f'{output_path}{name}_{step}_distribution.png')
    plt.clf()
    
    sns.lineplot(x=list(range(0, len(gt2_sorted_object_per_domain_list))), y=gt2_sorted_object_per_domain_list)    
    plt.xlabel("Domain ID")
    plt.ylabel("# of objects")
    plt.title("Objects per domain distribution")
    plt.savefig(f'{output_path}gt2_{name}_{step}_distribution.png')
    plt.clf()
    
    top100_domains_series.to_json(f'{output_path}{name}_{step}_top100_domains.json.gz')
    if len(low_domains_series) != 0:
        low_domains_series.to_json(f'{output_path}{name}_{step}_low_domains.json.gz')
    if len(rest_domains_series) != 0:
        rest_domains_series.to_json(f'{output_path}{name}_{step}_rest_domains.json.gz')


with open('../../schemaorg_classes.json', 'r') as f:
    class_dict = json.load(f)

classes = [k for k in class_dict.keys()]

for name in tqdm(classes):
    for k in class_dict.keys():
        if k == name:
            main_class = class_dict[k]
    
    print(main_class)
    
    output_path = f"../../data/interim/tablecorpus/domain_statistics/{name}/"
    shutil.rmtree(output_path, ignore_errors=True)
    Path(output_path).mkdir(parents=True, exist_ok=True)
    
    domains_all = glob.glob(f'../../data/interim/tablecorpus/domain_objects_extracted/all/{name}/*.pkl.gz')
    domains_delist = glob.glob(f'../../data/interim/tablecorpus/domain_objects_extracted/delist/{name}/*.pkl.gz')
    domains_delist_dedup = glob.glob(f'../../data/interim/tablecorpus/domain_objects_extracted/delist-dedup/{name}/*.pkl.gz')
    
    calc_stats(domains_all, name, main_class, output_path, 'all')
    calc_stats(domains_delist, name, main_class, output_path, 'delist')
    calc_stats(domains_delist_dedup, name, main_class, output_path, 'delist_dedup')