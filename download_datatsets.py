from hdx.data.dataset import Dataset
from hdx.facades.simple import facade
from hdx.utilities import is_valid_uuid
import pandas as pd
import argparse
import logging
from stat import S_ISDIR
import os
import traceback
import sys
import urllib3
import shutil
from os.path import join, expanduser

config=None
def main():
    global config
    df = pd.read_excel(config.table)
    log_df=pd.DataFrame(columns=[
      "dataset_name",
      "resource_name",
      "resource_url",
      "scraperwiki_name",
      "dir",
      "file",
      "status"
    ])

    c = urllib3.PoolManager()
    i=0
    for index,row in df.iterrows():
        i+=1
        dataset_name = row.dataset_name
        resource_name = row.resource_name
        print ("%(i)3d %(dataset_name)30s %(resource_name)30s"%locals())
        resource_url = row.resource_url
        localpath = os.path.join(config.target,dataset_name)
        localfile = os.path.join(localpath,resource_name)

        dataset = Dataset.read_from_hdx(dataset_name)
        print (dataset)

        if row.decision == config.decision:
            try:
                os.makedirs(localpath)
            except:
                pass
            logging.info("Process dataset %(dataset_name)s"%locals())
            logging.info("Fetch data from url %(dataset_name)s"%locals())
            try:
                with c.request('GET',resource_url, preload_content=False) as response, open(localfile, 'wb') as f:
                    shutil.copyfileobj(response, f)
                status="OK"
            except:
                logging.exception("Download error for dataset %(dataset_name)s"%locals())
                status="ERROR"
        else:
            status = "SKIPPED"

        log_df = log_df.append(dict(
            dataset_name = dataset_name,
            resource_name = resource_name,
            resource_url = resource_url,
            scraperwiki_name = row.scraperwiki_name,
            dir =localpath,
            file =localfile,
            status = status
        ),ignore_index=True)
        log_df.to_csv(config.processed)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table", help="Table of scrapers_ in xlsx format", default="datasets with scraperwiki.xlsx")
    parser.add_argument("--target", default='datasets', help="Target directory")
    parser.add_argument("--decision", default='make static', help="Process rows with decision")
    parser.add_argument("-l", "--log", help="Log file", default=None)
    parser.add_argument("-d", "--debug", action='store_true', help="Log debug messages.")
    parser.add_argument("-v", "--verbose", action='store_true', help="Increase verbosity.")
    parser.add_argument("--processed", default="processed_datasets.csv", help="Store table of processed entries to a csv file.")

    config = parser.parse_args()

    log_level = logging.WARNING
    if config.verbose:
        log_level = min(log_level, logging.INFO)
    if config.debug:
        log_level = min(log_level, logging.DEBUG)

    if config.log is not None:
        logging.basicConfig(filename=config.log, level=log_level)
    facade(main, hdx_site='prod', user_agent_config_yaml = join(expanduser('~'), '.dscheckuseragent.yml'))


