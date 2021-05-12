import requests
from zipfile import ZipFile
from pathlib import Path
import json

# Download schema.org nquads files as defined in schemaorg_classes.json for further processing into tables.

BASE_LINK = 'http://data.dws.informatik.uni-mannheim.de/structureddata/2020-12/quads/classspecific/'

if __name__ == "__main__":
    
    f = open('../../schemaorg_classes.json',)
    classes = json.load(f)
    
    types = ['md', 'json']
    
    for typ in types:
        
        Path(f'../../data/raw/tablecorpus/{typ}/').mkdir(parents=True, exist_ok=True)
        
        datasets = []

        for schema_class in classes.keys():
            datasets.append(f'{BASE_LINK}{typ}/schema_{schema_class}.gz')

        for link in datasets:

            # obtain filename by splitting url and getting
            # last string
            file_name = link.split('/')[-1]

            print("Downloading file:%s" % file_name)

            # create response object
            r = requests.get(link, stream=True)

            # download started
            with open(f'../../data/raw/tablecorpus/{typ}/{file_name}', 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

            print("%s downloaded!\n" % file_name)

    print("All files downloaded!")