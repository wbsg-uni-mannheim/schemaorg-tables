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
from statistics import quantiles, median
import json

from tqdm import tqdm
from pathlib import Path
import shutil
from urllib.parse import urlparse

import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from pdb import set_trace

## orders the table columns by global attribute count in the relevant schema.org class

# Set the amount of workers to pass to ProcessPoolExecutor or ThreadPoolExecutor
MAX_WORKERS = None

def write_tables(arg_tuple):
    
    domain_path, domain_name, length, columns, stats_dict, is_gt2, is_top100, is_rest, sorted_col_dict, output_path, threshold, name = arg_tuple
    

    table = pd.read_pickle(domain_path)
    
    current_table_order = []

    for k in sorted_col_dict.keys():
        if k in list(table.columns):
            current_table_order.append(k)

    current_table_order = ['row_id'] + current_table_order + ['page_url']

    table['row_id'] = table.index

    id_lookup = table[['row_id', 'node_id']]

    table = table.drop('node_id', axis=1)

    table = table[current_table_order]
    
    if is_top100:
        table.to_json(f'{output_path}top100/{name}_{domain_name}_September2020.json.gz', orient='records', lines=True, force_ascii=False)
        id_lookup.to_json(f'{output_path}top100/lookup/{name}_{domain_name}_September2020_lookup.json.gz', orient='records', lines=True, force_ascii=False)
    elif is_gt2:
        table.to_json(f'{output_path}gt2/{name}_{domain_name}_September2020.json.gz', orient='records', lines=True, force_ascii=False)
        id_lookup.to_json(f'{output_path}gt2/lookup/{name}_{domain_name}_September2020_lookup.json.gz', orient='records', lines=True, force_ascii=False)
    else:
        table.to_json(f'{output_path}rest/{name}_{domain_name}_September2020.json.gz', orient='records', lines=True, force_ascii=False)
        id_lookup.to_json(f'{output_path}rest/lookup/{name}_{domain_name}_September2020_lookup.json.gz', orient='records', lines=True, force_ascii=False)

def read_table(arg_tuple):
    domain_path, top_100 = arg_tuple
    
    domain_name = os.path.basename(domain_path)
    domain_name = domain_name[:-7]

    domain_table = pd.read_pickle(domain_path)
    
    is_top100 = True if domain_name in top_100 else False
    is_gt2 = len(domain_table) > 2 and not is_top100
    is_rest = True if not is_gt2 and not is_top100 else False
    
    densities = domain_table.notna().sum() * 100 / len(domain_table)
    density_dict = densities.to_dict()
    del density_dict['node_id']
    del density_dict['page_url']
    for k, v in deepcopy(density_dict).items():
        density_dict[k] = int(v)

    stats_dict = {
        'host':domain_name,
        'number_of_rows': len(domain_table),
        'column_count': len(density_dict),
        'column_name_and_density': density_dict
    }
    
    columns = list(domain_table.columns)
    length = len(domain_table)
    
    return (domain_path, domain_name, length, columns, stats_dict, is_gt2, is_top100, is_rest)
    
def sort_table_columns_and_write_statistics(name, main_class, output_path):
    
    attribute_count_dict = {}
    gt2_attribute_count_dict = {}
    top100_attribute_count_dict = {}
    rest_attribute_count_dict = {}
    
    table_store = []
    tables_overall = 0
    gt2_tables_overall = 0
    thresholds = [25]
    
    table_lengths = []
    table_lengths_top100 = []
    table_lengths_gt2 = []
    table_lengths_rest = []
    
    for threshold in thresholds:
        
        domains = glob.glob(f'../../data/interim/tablecorpus/domain_tables/{name}/{threshold}/*.pkl.gz')
        top_100 = pd.read_json(f'../../data/interim/tablecorpus/domain_statistics/{name}/{name}_delist_dedup_top100_domains.json.gz', typ='series')
        top_100 = list(top_100.index)

        stats_top100_df = pd.DataFrame()
        stats_gt2_df = pd.DataFrame()
        stats_rest_df = pd.DataFrame()
        stats_all_df = pd.DataFrame()
        
        arguments = ((domain, top_100) for domain in list(domains))
        table_store = []
        
        with tqdm(total=len(list(domains))) as pbar:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for arg_tuple in arguments:
                    futures.append(executor.submit(read_table, arg_tuple))

                for future in concurrent.futures.as_completed(futures):
                    table_store.append(future.result())
                    pbar.update(1)
        
        for result in tqdm(table_store):
            
            domain_path, domain_name, length, columns, stats_dict, is_gt2, is_top100, is_rest = result
            
            tables_overall += 1
            
            if is_gt2:
                gt2_tables_overall += 1
                  
            table_lengths.append(length)
            
            stats_all_df = stats_all_df.append(stats_dict, ignore_index=True)
            
            if is_top100:
                stats_top100_df = stats_top100_df.append(stats_dict, ignore_index=True)
                table_lengths_top100.append(length)
            elif is_gt2:
                stats_gt2_df = stats_gt2_df.append(stats_dict, ignore_index=True)
                table_lengths_gt2.append(length)
            else:
                stats_rest_df = stats_rest_df.append(stats_dict, ignore_index=True)
                table_lengths_rest.append(length)

            for col in columns:
                if col in attribute_count_dict.keys():
                    attribute_count_dict[col] += 1
                else:
                    attribute_count_dict[col] = 1
                    
                if is_gt2:
                    if col in gt2_attribute_count_dict.keys():
                        gt2_attribute_count_dict[col] += 1
                    else:
                        gt2_attribute_count_dict[col] = 1
                if is_top100:
                    if col in top100_attribute_count_dict.keys():
                        top100_attribute_count_dict[col] += 1
                    else:
                        top100_attribute_count_dict[col] = 1
                if is_rest:
                    if col in rest_attribute_count_dict.keys():
                        rest_attribute_count_dict[col] += 1
                    else:
                        rest_attribute_count_dict[col] = 1
                
        del attribute_count_dict['node_id']
        del attribute_count_dict['page_url']
        
        if len(gt2_attribute_count_dict) > 0:
            del gt2_attribute_count_dict['node_id']
            del gt2_attribute_count_dict['page_url']
        
        if len(top100_attribute_count_dict) > 0:
            del top100_attribute_count_dict['node_id']
            del top100_attribute_count_dict['page_url']
            
        if len(rest_attribute_count_dict) > 0:
            del rest_attribute_count_dict['node_id']
            del rest_attribute_count_dict['page_url']
            
        sorted_col_dict = {k: v for k, v in sorted(attribute_count_dict.items(), key=lambda x: x[1], reverse=True)}
        gt2_sorted_col_dict = {k: v for k, v in sorted(gt2_attribute_count_dict.items(), key=lambda x: x[1], reverse=True)}
        top100_sorted_col_dict = {k: v for k, v in sorted(top100_attribute_count_dict.items(), key=lambda x: x[1], reverse=True)}
        rest_sorted_col_dict = {k: v for k, v in sorted(rest_attribute_count_dict.items(), key=lambda x: x[1], reverse=True)}
        
        arguments = ((domain_path, domain_name, length, columns, stats_dict, is_gt2, is_top100, is_rest, sorted_col_dict, output_path, threshold, name) for (domain_path, domain_name, length, columns, stats_dict, is_gt2, is_top100, is_rest) in table_store)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(write_tables, arguments)   
        
        
        with open(f'{output_path}{name}_column_statistics.txt', 'w') as f:
            f.write('column,occurs_in_percentage_of_tables\n')
            for k, v in sorted_col_dict.items():
                f.write(f'{k},{"{:.2f}".format((v/len(table_lengths))*100)}\n')
                
        if len(gt2_attribute_count_dict) > 0:     
            with open(f'{output_path}gt2_{name}_column_statistics.txt', 'w') as f:
                f.write('column,occurs_in_percentage_of_tables\n')
                for k, v in gt2_sorted_col_dict.items():
                    f.write(f'{k},{"{:.2f}".format((v/len(table_lengths_gt2))*100)}\n')
                    
        if len(top100_attribute_count_dict) > 0:      
            with open(f'{output_path}top100_{name}_column_statistics.txt', 'w') as f:
                f.write('column,occurs_in_percentage_of_tables\n')
                for k, v in top100_sorted_col_dict.items():
                    f.write(f'{k},{"{:.2f}".format((v/len(table_lengths_top100))*100)}\n')
                    
        if len(rest_attribute_count_dict) > 0:
            with open(f'{output_path}rest_{name}_column_statistics.txt', 'w') as f:
                f.write('column,occurs_in_percentage_of_tables\n')
                for k, v in rest_sorted_col_dict.items():
                    f.write(f'{k},{"{:.2f}".format((v/len(table_lengths_rest))*100)}\n')
        
        
        quantiles_all = quantiles(table_lengths)
        if len(table_lengths_top100) > 0:
            quantiles_top100 = quantiles(table_lengths_top100)
        if len(table_lengths_gt2) > 0:
            quantiles_gt2 = quantiles(table_lengths_gt2)
        if len(table_lengths_rest) > 0:
            quantiles_rest = quantiles(table_lengths_rest)
        
        with open(f'{output_path}{name}_summary_statistics.txt', 'w') as f:
            f.write(f'statistic,value\nnumber of hosts,{len(table_lengths)}\nnumber of rows,{sum(table_lengths)}\nmedian rows per table,{median(table_lengths)}\nquantile borders,{quantiles_all}')
                
        if len(gt2_attribute_count_dict) > 0:     
            with open(f'{output_path}gt2_{name}_summary_statistics.txt', 'w') as f:
                f.write(f'statistic,value\nnumber of hosts,{len(table_lengths_gt2)}\nnumber of rows,{sum(table_lengths_gt2)}\nmedian rows per table,{median(table_lengths_gt2)}\nquantile borders,{quantiles_gt2}')
                    
        if len(top100_attribute_count_dict) > 0:      
            with open(f'{output_path}top100_{name}_summary_statistics.txt', 'w') as f:
                f.write(f'statistic,value\nnumber of hosts,{len(table_lengths_top100)}\nnumber of rows,{sum(table_lengths_top100)}\nmedian rows per table,{median(table_lengths_top100)}\nquantile borders,{quantiles_top100}')
                    
        if len(rest_attribute_count_dict) > 0:
            with open(f'{output_path}rest_{name}_summary_statistics.txt', 'w') as f:
                f.write(f'statistic,value\nnumber of hosts,{len(table_lengths_rest)}\nnumber of rows,{sum(table_lengths_rest)}\nmedian rows per table,{median(table_lengths_rest)}\nquantile borders,{quantiles_rest}')
        
        column_order = ['host', 'number_of_rows', 'column_count', 'column_name_and_density']
        
        stats_all_df['number_of_rows'] = stats_all_df['number_of_rows'].astype(int)
        stats_all_df['column_count'] = stats_all_df['column_count'].astype(int)

        stats_top100_df['number_of_rows'] = stats_top100_df['number_of_rows'].astype(int)
        stats_top100_df['column_count'] = stats_top100_df['column_count'].astype(int)
        
        if len(stats_gt2_df) > 0:
            stats_gt2_df['number_of_rows'] = stats_gt2_df['number_of_rows'].astype(int)
            stats_gt2_df['column_count'] = stats_gt2_df['column_count'].astype(int)
        
        if len(stats_rest_df) > 0:
            stats_rest_df['number_of_rows'] = stats_rest_df['number_of_rows'].astype(int)
            stats_rest_df['column_count'] = stats_rest_df['column_count'].astype(int)
        
        stats_all_df[column_order].sort_values(by='number_of_rows', ascending=False).to_csv(f'{output_path}{name}_September2020_statistics.csv', index=False)
        stats_top100_df[column_order].sort_values(by='number_of_rows', ascending=False).to_csv(f'{output_path}{name}_September2020_statistics_top100.csv', index=False)
        if len(stats_gt2_df) > 0:
            stats_gt2_df[column_order].sort_values(by='number_of_rows', ascending=False).to_csv(f'{output_path}{name}_September2020_statistics_gt2.csv', index=False)
        if len(stats_rest_df) > 0:
            stats_rest_df[column_order].sort_values(by='number_of_rows', ascending=False).to_csv(f'{output_path}{name}_September2020_statistics_rest.csv', index=False)


with open('../../schemaorg_classes.json', 'r') as f:
    class_dict = json.load(f)
    
classes = [k for k in class_dict.keys()]


for name in classes:
    for k in class_dict.keys():
        if k == name:
            main_class = class_dict[k]
    
    output_path = f"../../data/processed/tablecorpus/domain_tables_sorted/{name}/"
    shutil.rmtree(output_path, ignore_errors=True)
    Path(f'{output_path}top100/lookup/').mkdir(parents=True, exist_ok=True)
    Path(f'{output_path}rest/lookup/').mkdir(parents=True, exist_ok=True)
    Path(f'{output_path}gt2/lookup/').mkdir(parents=True, exist_ok=True)
    
    sort_table_columns_and_write_statistics(name, main_class, output_path)