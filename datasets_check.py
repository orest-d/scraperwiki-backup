from hdx.data.dataset import Dataset
from hdx.facades.simple import facade
from hdx.utilities import is_valid_uuid
import pandas as pd
from os.path import join, expanduser

def main():
    datasets = Dataset.get_all_datasets()
    data=[]

    for dataset in datasets:
        maintainer = dataset['maintainer']
        if not is_valid_uuid(maintainer):
            org = dataset.get('organization')
            if org:
                org = org['title']
            else:
                org = 'NONE!'
            if not maintainer:
                maintainer = 'NONE!'
        resources = Dataset.get_all_resources(datasets)
        name = dataset['name']
        data.append(dict(dataset_name=name,maintainer=maintainer,org=org))
        df=pd.DataFrame(data)
        df.to_csv("dataset.csv",index=False)
if __name__ == '__main__':
    facade(main, hdx_site='prod', user_agent_config_yaml = join(expanduser('~'), '.dscheckuseragent.yml'))
