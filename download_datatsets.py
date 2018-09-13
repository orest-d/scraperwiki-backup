from distutils.command.upload import upload
from urllib.response import addbase

from dns import update
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
import urllib3.util as ut
import shutil
from os.path import join, expanduser

config = None


def resource_number_from_url(dataset, resource_url):
    for i, r in enumerate(dataset.get_resources()):
#        if r["url"] == resource_url:
        if r["url"] == resource_url or config.new_url_pattern in r["url"]:
            return i

def resource_from_name(dataset, resource_name):
    for r in dataset.get_resources():
        if r["name"] == resource_name:
            return r


def main():
    global config
    df = pd.read_excel(config.table)
    log_df = pd.DataFrame(columns=[
        "dataset_name",
        "resource_name",
        "resource_filename",
        "resource_url",
        "new_resource_url",
        "scraperwiki_name",
        "dir",
        "file",
        "status",
        "update_status",
    ])

    c = urllib3.PoolManager()
    i = 0
    additional_df=pd.DataFrame(columns=[
        "decision",
        "dataset_name",
        "resource_name",
        "resource_url"
    ])
    for resource_index, row in df.iterrows():
        i += 1
        dataset_name = row.dataset_name
        resource_name = str(row.resource_name)
        # print ("%(i)3d %(dataset_name)30s %(resource_name)30s"%locals())
        resource_url = row.resource_url
        if resource_name.endswith(".csv") or resource_name.endswith(".xls") or resource_name.endswith(".xlsx"):
            resource_filename = resource_name
        else:
            resource_filename = os.path.split(ut.parse_url(resource_url).path)[1]

        # localpath = os.path.join(os.path.join(config.target,dataset_name),resource_name)
        localpath = os.path.join(config.target, dataset_name)
        localfile = os.path.join(localpath, resource_filename)
        new_resource_url = None

        update_status = ""
        scraperwiki_resources=[]
        if row.decision == config.decision:
            new_resource_url = config.url_prefix
            if new_resource_url[-1] != "/":
                new_resource_url += "/"
            new_resource_url += dataset_name + "/" + resource_filename
            if config.new_url_pattern not in new_resource_url:
                logging.warning("New url '%s' does not contain the new-url-pattern '%s'"%(new_resource_url,config.new_url_pattern))

            dataset = Dataset.read_from_hdx(dataset_name)
            resource_index = resource_number_from_url(dataset, resource_url)
            for i,r in enumerate(dataset.get_resources()):
                if (config.old_url_pattern in r["url"] or config.new_url_pattern in r["url"]) and i!=resource_index:
                    additional_df = additional_df.append(dict(
                        decision = row.decision,
                        dataset_name = dataset_name,
                        resource_name = r["name"],
                        resource_url = r["url"]
                    ),ignore_index=True)
            additional_df.to_csv(config.additional,index_label='Index')

            if config.update_url:
                logging.info("Update url %(dataset_name)s, resource: %(resource_name)s to %(new_resource_url)s" % locals())

                try:
                    resource = resource_from_name(dataset, resource_name)
                    if resource is None:
                        update_status = "RESOURCE NOT FOUND"
                    else:
                        resource["url"] = new_resource_url
                        resource.update_in_hdx()
                        update_status = "OK"
                except:
                    logging.error("Update url failed for %(dataset_name)s resource %(resource_name)s" % locals())
                    update_status = "ERROR"
                    traceback.print_exc()
            try:
                os.makedirs(localpath)
            except:
                pass
            logging.info("Process dataset %(dataset_name)s" % locals())
            logging.info("Fetch data from url %(dataset_name)s" % locals())
            if config.refresh or not os.path.exists(localfile):
                try:
                    with c.request('GET', resource_url, preload_content=False) as response, open(localfile, 'wb') as f:
                        shutil.copyfileobj(response, f)
                    status = "OK"
                except:
                    logging.exception("Download error for dataset %(dataset_name)s" % locals())
                    status = "ERROR"
            else:
                status = "Ok"

        else:
            status = "SKIPPED"

        log_df = log_df.append(dict(
            dataset_name=dataset_name,
            resource_name=resource_name,
            resource_filename=resource_filename,
            resource_url=resource_url,
            new_resource_url=new_resource_url,
            scraperwiki_name=row.scraperwiki_name,
            dir=localpath,
            file=localfile,
            status=status,
            update_status=update_status
        ), ignore_index=True)
        log_df.to_csv(config.processed,index_label='Index')
    additional_df["additional"]=1
    df["additional"]=0
    df = df.append(additional_df,ignore_index=True)
    writer = pd.ExcelWriter(config.additional_table)
    df.to_excel(writer)
    writer.save()
    writer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table", help="Table of scrapers_ in xlsx format",
                        default="datasets with scraperwiki.xlsx")
    parser.add_argument("--target", default='datasets', help="Target directory")
    parser.add_argument("--decision", default='make static', help="Process rows with decision")
    parser.add_argument("-r", "--refresh", action='store_true',
                        help="Always download resources (even if they exist locally)")
    parser.add_argument("--url-prefix",
                        default='https://ocha-dap.github.io/scraperwiki-snapshot/datasets/',
                        help="URL Prefix before to be connected with dataset and resource name")
    parser.add_argument("-u", "--update-url", action='store_true', help="Update resource url for selected datasets")
    parser.add_argument("--hdx-site", default="test",
                        help="HDX site (test, prod, ...)")
    parser.add_argument("--processed", default="processed_datasets.csv",
                        help="Store table of processed entries to a csv file.")
    parser.add_argument("--additional", default="additional_resource.csv",
                        help="Store table of additional resources identified to a csv file.")
    parser.add_argument("--additional-table", default="datasets with scraperwiki_add.xlsx",
                        help="Store table of original with additional resources to a xlsx file.")
    parser.add_argument("--old-url-pattern", default="scraperwiki.com",
                        help="String present in old URLs")
    parser.add_argument("--new-url-pattern", default="scraperwiki-snapshot/datasets",
                        help="String present in new URLs")
    parser.add_argument("-l", "--log", help="Log file", default=None)
    parser.add_argument("-d", "--debug", action='store_true', help="Log debug messages.")
    parser.add_argument("-v", "--verbose", action='store_true', help="Increase verbosity.")
    config = parser.parse_args()

    log_level = logging.WARNING
    if config.verbose:
        log_level = min(log_level, logging.INFO)
    if config.debug:
        log_level = min(log_level, logging.DEBUG)

    if config.log is not None:
        logging.basicConfig(filename=config.log, level=log_level)
    facade(main, hdx_site=config.hdx_site, user_agent_config_yaml=join(expanduser('~'), '.dscheckuseragent.yml'))
