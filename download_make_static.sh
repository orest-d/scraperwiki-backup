#!/usr/bin/env bash

../venv/bin/python3 download.py -t table_make_static.csv -v --log download_make_static_log.txt --ignore-patterns ignore_less_patterns.txt --filelist download_make_static_files.csv --processed processed_make_static.csv --target make_static