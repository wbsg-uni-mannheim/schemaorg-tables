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

import statistics

from tqdm import tqdm
from pathlib import Path
import shutil
from urllib.parse import urlparse

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from pdb import set_trace

## Parses over domain-wise nquad files and extracts all attributes and sub-attributes for objects from pipeline step 2
## Also performs listing page removal and deduplication steps

# Set the amount of workers to pass to ProcessPoolExecutor or ThreadPoolExecutor
MAX_WORKERS = None

def calculate_median_absolute_deviation(numbers_list):
    median = statistics.median(numbers_list)
    deviation_list = [abs(x-median) for x in numbers_list]
    median_absolute_deviation = statistics.median(deviation_list)
    return median, median_absolute_deviation

def get_object_summary_word_list(identifier, object_dict):
    summary_attr = []
    for attribute, value in object_dict[identifier].items():
                if type(value) == str:
                    summary_attr.extend(value.split())
                else:
                    for subvalue in value:
                        if type(subvalue) == str:
                            summary_attr.extend(subvalue.split())
                        else:
                            for subsubvalue in subvalue:
                                if len(subsubvalue) == 1:
                                    summary_attr.extend(list(subsubvalue)[0][1].split())
                                else:
                                    for subsubsubvalue in subsubvalue:
                                        summary_attr.extend(subsubsubvalue[1].split())
    
    return summary_attr

def extract_objects(arg_tuple):
    
    domain_path, domain_name, name, main_class, output_path_dict, regexp = arg_tuple

    with gzip.open(f'../../data/interim/tablecorpus/domain_objects/{name}/{domain_name}.pkl.gz',"rb") as f:
        domain_dict = pickle.load(f)

####################################################################
## Attribute Extraction

    object_dict = {}
    node_ids_set = set()
    
    secondary_attributes_to_extract = {}
    secondary_objects_dict = {}
    
    paths_to_replace = set()
    
    with gzip.open(domain_path, 'rt', encoding='utf-8') as f:
        for i, line in enumerate(f):
            line = line.rstrip()
            split = line.split()
            
            url = split[-2]
            url = url[1:-1]
            
            if url in domain_dict:
                
                node_id = split[0]
                
                node_ids_set.add(node_id)
                
                if node_id in domain_dict[url]:
                    
                    value = ' '.join(split[2:-2])
                    
                    so_class = value[1:-1]
                    so_class = so_class.replace('https', 'http')
                    so_class = so_class.lower()
                    
                    if so_class == main_class:
                        continue
                    
                    regexp_search = regexp.search(value)
                    
                    is_node_bool = False
                    is_other_bool = False
                    is_literal_bool = bool(regexp_search)
                    
                    if not is_literal_bool:
                        is_node_bool = True
                    
                    if 'http://schema.org/' in so_class and not is_literal_bool:
                        continue
                    
                    attribute = split[1]
                    
                    if 'schema.org' not in attribute:
                        continue
                    
                    attribute = attribute[1:-1]
                    attribute = attribute.split('/')[-1]
                    attribute = attribute.lower()
                 
                    if not is_literal_bool and not is_node_bool:
                        is_other_bool = True
                        print('Shouldnt happen!')
                        set_trace()
                        
                    object_id = (node_id, url)
                    
                    if is_literal_bool:
            
                        value_clean = regexp_search.group(0)
                        value_clean = value_clean.strip('"')
                    
                        for char in ['\\t', '\\n', '\\r']:
                            value_clean = value_clean.replace(char, ' ')
                        value_clean = ' '.join(value_clean.split()[:200])
                        
                        if value_clean == '""' or value_clean == '"' or value_clean == '':
                            continue
                        
                        try: 
                            regexp_search.group(1)
                            set_trace()
                        except:
                            pass
                        
                        if object_id not in object_dict.keys():
                            object_dict[object_id] = {}
                        if attribute not in object_dict[object_id].keys():
                            object_dict[object_id][attribute] = set([value_clean])
                        else:
                            if len(object_dict[object_id][attribute]) < 50:
                                object_dict[object_id][attribute].add(value_clean)
                        
                    elif is_node_bool:
                        
                        if url not in secondary_attributes_to_extract.keys():
                            secondary_attributes_to_extract[url] = set()
                        secondary_attributes_to_extract[url].add(value) 
                        
                        if object_id not in object_dict.keys():
                            object_dict[object_id] = {}
                        if attribute not in object_dict[object_id].keys():
                            object_dict[object_id][attribute] = set([(value, url)])
                        if len(object_dict[object_id][attribute]) < 50:
                            paths_to_replace.add((object_id, attribute, (value,url)))
                            object_dict[object_id][attribute].add((value, url))
    
    for (object_id, attribute, (value,url)) in deepcopy(paths_to_replace):
        if value not in node_ids_set:
            
            paths_to_replace.remove((object_id, attribute, (value,url)))
            try:
                secondary_attributes_to_extract[url].remove(value)
            except KeyError:
                object_dict[object_id][attribute].remove((value, url))
                continue
                
            object_dict[object_id][attribute].remove((value, url))
            
            if len(secondary_attributes_to_extract[url]) == 0:
                del secondary_attributes_to_extract[url]
    
    
    with gzip.open(domain_path, 'rt', encoding='utf-8') as f:
        for i, line in enumerate(f):
            line = line.rstrip()
            split = line.split()
            
            url = split[-2]
            url = url[1:-1]
            
            if url in secondary_attributes_to_extract:
                node_id = split[0]
                
                if node_id in secondary_attributes_to_extract[url]:
                    
                    value = ' '.join(split[2:-2])
                    
                    so_class = value[1:-1]
                    so_class = so_class.replace('https', 'http')
                    
                    regexp_search = regexp.search(value)
                    
                    is_node_bool = False
                    is_other_bool = False
                    is_literal_bool = bool(regexp_search)
                    
                    if not is_literal_bool:
                        is_node_bool = True 
                                       
                    if 'http://schema.org/' in so_class and not is_literal_bool:
                        continue
                    
                    attribute = split[1]
                    
                    if 'schema.org' not in attribute:
                        continue
                    
                    attribute = attribute[1:-1]
                    attribute = attribute.split('/')[-1]
                    attribute = attribute.lower()
                    
                    
                    if not is_literal_bool and not is_node_bool:
                        is_other_bool = True
                        print('Shouldnt happen!')
                        set_trace()
                        
                    if is_literal_bool:
                        
                        value_clean = regexp_search.group(0)
                        value_clean = value_clean.strip('"')
                    
                        for char in ['\\t', '\\n', '\\r']:
                            value_clean = value_clean.replace(char, ' ')
                        value_clean = ' '.join(value_clean.split()[:200])
                        
                        if value_clean == '""' or value_clean == '"' or value_clean == '':
                            continue
                        
                        try: 
                            regexp_search.group(1)
                            set_trace()
                        except:
                            pass
                        
                        value_clean = (attribute, value_clean)
                        
                        object_id = (node_id, url)
                        if object_id not in secondary_objects_dict.keys():
                            secondary_objects_dict[object_id] = {}
                        if attribute not in secondary_objects_dict[object_id].keys():
                            secondary_objects_dict[object_id][attribute] = set()
                            
                        secondary_objects_dict[object_id][attribute].add(value_clean)
                        
                        
    secondary_objects_dict_cleaned = deepcopy(secondary_objects_dict)
    

    for path in paths_to_replace:
        (object_id, attribute, identifier) = path
        
        try:
            object_dict[object_id][attribute].remove(identifier)
        except KeyError:
            set_trace()
        try:
            to_add_dict = secondary_objects_dict_cleaned[identifier]
        except KeyError:
            if len(object_dict[object_id][attribute]) == 0:
                del object_dict[object_id][attribute]          
            continue
        to_add_set = set()
        for k, v in to_add_dict.items():
            to_add_set.add(frozenset(v))
            
        to_add_set = frozenset(to_add_set)
        if len(object_dict[object_id][attribute]) < 50:
            object_dict[object_id][attribute].add(to_add_set)
        
    object_dict_copy = deepcopy(object_dict)
    
    for object_id, obj in object_dict_copy.items():
        for attribute, value in obj.items():
            if len(value) == 0:
                del object_dict[object_id][attribute]
        if len(object_dict[object_id]) == 0 :
            del object_dict[object_id]
    
    if len(object_dict) == 0:
        return
    
    with gzip.open(f"{output_path_dict['all']}{domain_name}.pkl.gz","wb") as f:
        pickle.dump(object_dict, f)

####################################################################
## Delisting
        
    url_lookup_dict = {}
    for object_id, obj in object_dict.items():
        if object_id[1] in url_lookup_dict.keys():
            url_lookup_dict[object_id[1]].append(object_id)
        else:
            url_lookup_dict[object_id[1]] = [object_id]
    
    delist_object_dict = deepcopy(object_dict)
    
    debug_to_next_domain = False
    
    for url, object_list in url_lookup_dict.items():
        
        global page_counter
        
        if debug_to_next_domain:
            break
        if len(object_list) == 1:
            
            page_counter += 1
            ## check for at least 3 attributes
            identifier = object_list[0]
            object_attr_counts = len(object_dict[identifier])
            
            if object_attr_counts < 3:
                del delist_object_dict[identifier]

        else:
            global multi_obj_counter
            multi_obj_counter += 1
            page_counter += 1
            ## if there are multiple objects, additionally use MAD outlier detection
            object_attr_counts_list = []
            object_attr_length_list = []
            for identifier in object_list:

                words = get_object_summary_word_list(identifier, object_dict)
                words_length = len(words)

                object_attr_length_list.append(words_length)
                object_attr_counts_list.append(len(object_dict[identifier]))

            max_attr_length = max(object_attr_length_list)
            max_attr_count = max(object_attr_counts_list)

            if object_attr_counts_list.count(max_attr_count) == len(object_attr_counts_list) and object_attr_length_list.count(max_attr_length) == len(object_attr_length_list):
                
                main_object = object_dict[object_list[0]]
                for identifier in object_list[1:]:
                    if object_dict[identifier] == main_object and object_attr_counts_list[0] > 2:
                        del delist_object_dict[identifier]
                    else:
                        del delist_object_dict[identifier]
                        if object_list[0] in delist_object_dict.keys():
                            del delist_object_dict[object_list[0]]
            else:
                if len(object_list) == 2:
                    global two_obj_counter
                    two_obj_counter += 1
                median, mad = calculate_median_absolute_deviation(object_attr_length_list)
                is_positive_outlier_list = [1 if median + 3*mad < x else 0 for x in object_attr_length_list]
                if is_positive_outlier_list.count(1) > 0:
                    indices = [index for index, count in enumerate(is_positive_outlier_list) if count > 0]
                else:
                    indices = []
                    global all_removed_counter
                    all_removed_counter += 1
                for i, identifier in enumerate(object_list):
                    if i in indices and object_attr_counts_list[i] > 2:
                        continue
                    else:
                        del delist_object_dict[identifier]
    
    if len(delist_object_dict) == 0:
        return
    
    with gzip.open(f"{output_path_dict['delist']}{domain_name}.pkl.gz","wb") as f:
        pickle.dump(delist_object_dict, f)

####################################################################        
## Deduplication

    frozen_object_dict = {}
    dedup_delist_object_dict = {}
    dup_set = set()
    
    for object_id, obj in delist_object_dict.items():
        
        frozen_object_dict = {}
        
        for k, v in obj.items():
            
            if type(v) == str:
                set_trace()
                frozen_object_dict[k] = frozenset(v)
            else:
                new_value = set()
                for subvalue in v:
                    if type(subvalue) == str:
                        new_value.add(subvalue)
                    else:
                        new_subvalue = set()
                        for subsubvalue in subvalue:
                            if len(subsubvalue) == 1:
                                current_tuple = list(subsubvalue)[0]
                                if current_tuple[0] != 'url':
                                    new_subvalue.add(subsubvalue)
                            else:
                                new_subsubvalue = set()
                                for subsubsubvalue in subsubvalue:
                                    if subsubsubvalue[0] != 'url':
                                        new_subsubvalue.add(subsubsubvalue)
                                if len(new_subsubvalue) != 0:
                                    new_subvalue.add(frozenset(new_subsubvalue))
                        if len(new_subvalue) != 0:
                            new_value.add(frozenset(new_subvalue))
                if len(new_value) != 0:
                    frozen_object_dict[k] = frozenset(new_value)
            
        frozen_object_dict.pop('url', None)
        
        frozen_object_set = frozenset(frozen_object_dict.items())
        
        if frozen_object_set not in dup_set:
            dup_set.add(frozen_object_set)
            dedup_delist_object_dict[object_id] = delist_object_dict[object_id]
    
    if len(dedup_delist_object_dict) == 0:
        return
    
    with gzip.open(f"{output_path_dict['delist-dedup']}{domain_name}.pkl.gz","wb") as f:
        pickle.dump(dedup_delist_object_dict, f)


with open('../../schemaorg_classes.json', 'r') as f:
    class_dict = json.load(f)

classes = [k for k in class_dict.keys()]

regexp = re.compile(r'".*"')

for name in tqdm(classes):
    
    page_counter = 0
    multi_obj_counter = 0
    two_obj_counter = 0
    all_removed_counter = 0
    
    for k in class_dict.keys():
        if k == name:
            main_class = class_dict[k].lower()
    
    output_path_dict = {
                        'all': f"../../data/interim/tablecorpus/domain_objects_extracted/all/{name}/",
                        'delist': f"../../data/interim/tablecorpus/domain_objects_extracted/delist/{name}/",
                        'delist-dedup': f"../../data/interim/tablecorpus/domain_objects_extracted/delist-dedup/{name}/"
                       }

    for tag, path in output_path_dict.items():
        shutil.rmtree(path, ignore_errors=True)
        Path(path).mkdir(parents=True, exist_ok=True)
    
    domains = glob.glob(f'../../data/raw/tablecorpus/by-domain/{name}/*.gz')
    
    arguments = ((v, os.path.basename(v)[:-3], name, main_class, output_path_dict, regexp) for v in list(domains))
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(extract_objects, arguments)
    
    try:
        print(f'Class: {name}, pages with 2 objects over all pages: {"{:.2f}".format(two_obj_counter/page_counter*100)}%\n')        
    except ZeroDivisionError:
        pass
    try:
        print(f'Class: {name}, pages with multiple objects completely removed: {"{:.2f}".format(all_removed_counter/multi_obj_counter*100)}%\n')
    except ZeroDivisionError:
        pass