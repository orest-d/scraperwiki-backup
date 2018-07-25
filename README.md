# Backup of the ScraperWiki

This repository contains the backup script and the actual backup of the
UN OCHA's ScraperWiki.

Download can be performed by the download.py script, which is driven by the table.csv.
(All table names may be changed via command-line options, e.g. table.csv 
can be changed to something else by "-t" or "--table" options.)
The table.csv must contain at least two columns:
"id" and "Name". This table can be created by scrapping the wiki list and converting it using htmltocsv.py script.
This table can be used to control partial downloads, which is useful e.g. if a large download is interrupted.
For that purpose code looks into the "download_status" column. If it is not presents
or if it does not contain "OK", download is performed.
(So download of a particular scraper can be restarted by removing the "OK" in its row.)

Files are downloaded into target directory (scrapers by default), each scraper into
it's own folder either specified in the "directory" column or derived from
the "Name" column. (So the target folder can be controlled by "--target"
command line option and the subfolder by the "directory" column in the table.csv.) 

As a result script produces a "processed table" (by default "processed.csv").
This table copies the original table.csv and adds more columns - particularly the download_status and directory.
Thus processed.csv can be used instead of table.csv to process missing or errorneous downloads.

List of all files processed is written by default to files.csv.
Some files may be ignored, and the reason as well as eventual errors are listed in the tables.

Files may be ignored for these reasons:
1. File is in a folder listed in ignore_folders.txt. Such folders are not processed
and thus thy will not appear in the files.csv.
2. File may conform to a pattern listed in ignore_patterns.txt. File is then listed,
but it is ignored and it's size will be listed as zero.
3. File size may be above the size limit (if specified with --size-limit).
4. An error happened.
  
There are several example scripts:
* __help.sh__ : Lists the command-line parameters
* __scan.sh__ : Get the list of files without downloading them.
* __download_reduced.sh__ : Download a reduced set of files.
* __download_more.sh__ : Download a larger set of files - e.g. no size limit and include sqlite files. 