# schemaorg-tables

This repository contains the code and data download links to reproduce the building process of the 2021 [Schema.org Table Corpus](http://webdatacommons.org/structureddata/schemaorgtables/).

* **Requirements**

    [Anaconda3](https://www.anaconda.com/products/individual)

    Please keep in mind that the code is not optimized for portable or non-workstation devices. The construction pipeline contains multiple steps with output generated for every step. If you want to regenerate the whole corpus multiple Terabytes of disk space or code optimizations are required.

    The code has only been used and tested on Linux (CentOS) servers.

* **Building the conda environment**

    To build the exact conda environment used for building the corpus, navigate to the project root folder where the file ```schemaorg-tables.yml``` is located and run ```conda env create -f schemaorg-tables.yml```

* **Downloading the raw schema.org nquad data files**
	
	You can specify for which schema.org classes you want to build the tables, which subsequently also impacts the file downloads: This is possible by editing the file ```schemaorg_classes.json``` in the root directory to your liking. By default, tables are created for all schema.org classes found [here](http://webdatacommons.org/structureddata/2020-12/stats/schema_org_subsets.html)
    
    Navigate to the ```src/data/``` folder and run ```python download_datasets.py``` to automatically download the raw files into the correct locations.
    You can find the data at ```data/raw/tablecorpus/```.

* **Running the pipeline**

    To convert the data from raw nquads to the tables of the Schema.org Table corpus, navigate to the ```src/tablecorpus/``` folder and run the following files in order (also signified by the leading number in the filename):
    
    **Warning**: Running this pipeline for all classes, especially if you include the larger ones like Products or Person can take several days/weeks.
    
    1. ```src/tablecorpus/01_split_domain_files.py```
    2. ```src/tablecorpus/02_get_objects_from_domain_files.py```
    3. ```src/tablecorpus/03_extract_attributes_domainwise.py```
    4. ```src/tablecorpus/04a_build_tables.py```
    5. ```src/tablecorpus/04b_calculate_statistics.py```
    6. ```src/tablecorpus/05-order-by-global-attribute-count.py```
    7. ```src/tablecorpus/06-write-sample-tables.py```
    8. ```src/tablecorpus/07-rename-statsfiles.py```
    9. ```src/tablecorpus/08-zip-tables-release.py```
    
    Steps 4 and 5 can be run concurrently as they do not depend on each other. After running all files the final zipped tables/samples/statistics files can be found at ```data/processed/tablecorpus/domain_tables_upload```.
    
    Note: some of the scripts allow you to set a MAX_WORKERS variable. This is set to ```None``` by default which uses pythons default value. You should consider adapting this to suit your machine and speed up the process.

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
