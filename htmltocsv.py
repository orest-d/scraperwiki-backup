# -*- coding: utf-8 -*-
from __future__ import print_function
import os

from bs4 import BeautifulSoup
import argparse
import logging
import pandas as pd


def extract_raw_table(soup_table):
    "Convert soup table to a list of lists with raw data."
    data=[]
    def isitem(x):
        try:
            name=x.name.lower()
        except:
            return False
        return name in ["td","th"]
        
    for html_row in soup_table.find_all('tr'):
        row = ["id"]+[" ".join(element.stripped_strings) for element in html_row if isitem(element)][1:]
        try:
            row[0] = html_row.attrs["data-box"]
        except:
            pass
        data.append(row)
    return data


def extract_header(soup_table):
    "Convert soup table to a list of lists with raw data."
    data = []

    def isitem(x):
        try:
            name = x.name.lower()
        except:
            return False
        return name in ["th", "td"]

    for html_row in soup_table.find_all('th'):
        data.append(" ".join(html_row.stripped_strings))
    return data


def csv_format_item(item):
    if item is None:
        return ""
    else:
        return str(item).replace("\n","")

def process(page,csv="table.csv"):
    soup = BeautifulSoup(page, 'html.parser')
    table = extract_raw_table(soup)
#    with open(csv, "w") as f:
#        for row in table:
#            f.write(";".join(csv_format_item(item) for item in row))
#            f.write("\n")
    df = pd.DataFrame(table[1:],columns=table[0])
    df.to_csv(csv,index=False)
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--input", help="Input file in html format", default="table.html")
    parser.add_argument("-l","--log",   help="Log file", default=None)
    parser.add_argument("-d", "--debug",   action='store_true', help="Log debug messages.")
    parser.add_argument("-v", "--verbose", action='store_true', help="Increase verbosity.")
    parser.add_argument("--csv", default="table.csv", help="Store output to a csv file.")

    config = parser.parse_args()
    
    log_level = logging.WARNING
    if config.verbose:
        log_level = min(log_level,logging.INFO)
    if config.debug:
        log_level = min(log_level,logging.DEBUG)
        
    if config.log is not None:
        logging.basicConfig(filename=config.log,level=log_level)
    
    try:
        page = open(config.input).read()
    except:
        logging.exception("Can't read page %s"%url)
        print ("Can't read page %s"%url)
        exit(0)

    logging.info("Processing started")
    process(page,csv=config.csv)
    logging.info("Processing finished")
