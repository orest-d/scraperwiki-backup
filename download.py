from paramiko import SSHClient
from scp import SCPClient
import pandas as pd
import argparse
import logging
from stat import S_ISDIR
import os
import traceback
import fnmatch

def load_scp_test():
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect('ds-ec2.scraperwiki.com',username='xnemvl4')

    # SCPCLient takes a paramiko transport as an argument
    scp = SCPClient(ssh.get_transport())

    #scp.put('test.txt', 'test2.txt')
    scp.get('log.txt')

    # Uploading the 'test' directory with its content in the
    # '/home/user/dump' remote directory
    #scp.put('test', recursive=True, remote_path='/home/user/dump')

    scp.close()
    ssh.close()

def load_sftp_test():
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect('ds-ec2.scraperwiki.com',username='xnemvl4')
    sftp = ssh.open_sftp()
    # Download

    filepath = 'log.txt'
    localpath = 'log.txt'
    sftp.get(filepath, localpath)

    sftp.close()
    ssh.close()

ignore_folders = [
    ".cache",
    ".git",
    "venv",
    "__pycache__",
    ".vim",
    ".local",
    "R",
    ".pip",
    ".subversion",
    "virtualenvs",
    ".virtualenvs",
    "newvenv",
    "oldvenv",
    "py3env",
    "pip-RcFOfd-uninstall",
    "R-3.1.1"
]

ignore_patterns = [
  "*.sqlite-journal",
  "*.log",
  "log.txt",
  "*/log.txt",
  "scraperwiki.sqlite.20170721.bk",
  "*/scraperwiki.sqlite.20170721.bk",
  "cron.log.save",
  "*.so",
  "*.tar.gz",
  "*.tgz",
  "*.pyc",
  "*.sqlite.bk",
  "*tmp/tmp*"
]

def ignore_file(path):
    for pattern in ignore_patterns:
        if fnmatch.fnmatchcase(path,pattern):
            return True, pattern
    return False,""

def sftp_walk(sftp,remotepath):
    path = remotepath
    files = []
    folders = []
    for f in sftp.listdir_attr(remotepath):
        if S_ISDIR(f.st_mode):
            folders.append(f.filename)
        else:
            files.append(f.filename)
    if files:
        yield path, files
    for folder in folders:
        new_path = os.path.join(remotepath, folder)
        if folder in ignore_folders:
            logging.warning("Folder ignored: "+new_path)
        else:
            for x in sftp_walk(sftp,new_path):
                yield x

def sftp_download(host, username, password=None, target_directory=".",size_limit=None):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    if password is None:
        ssh.connect(host,username=username)
    else:
        ssh.connect(host, username=username,password=password)
    sftp = ssh.open_sftp()

    count = 0
    total_size = 0

    file_list = []

    for path, files in sftp_walk(sftp,"."):
        for file in files:
            localpath = os.path.normpath(os.path.join(target_directory,path))
            remote_file = os.path.join(path, file)
            local_file = os.path.normpath(os.path.join(localpath,file))
            ignore, reason = ignore_file(remote_file)
            if ignore:
                size=0
                status="OK"
                ignored=1
            else:
                try:
                    size=sftp.stat(remote_file).st_size
                    if size_limit>0 and size>size_limit:
                        status = "OK"
                        ignored = 1
                        reason = "SIZE"
                    else:
                        sftp.get(remote_file, local_file)
                        status = "OK"
                        ignored = 0
                except:
                    logging.exception("Error getting size: %s"%remote_file)
                    size = 0
                    status = "ERROR"
                    ignored=1
                    reason="ERROR"

                # sftp.get(remote, local) line for dowloading.
                try:
                    os.makedirs(localpath)
                except:
                    pass
                total_size+=size
                file_list.append(dict(id=username,path=path,file=file,file_path=remote_file,size=size, status=status, ignored=ignored, ignore_reason=reason))

            count+=1

    sftp.close()
    ssh.close()
    return count, size, file_list

def sftp_list(host, username, password=None):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    if password is None:
        ssh.connect(host,username=username)
    else:
        ssh.connect(host, username=username,password=password)
    sftp = ssh.open_sftp()

    file_list = []
    for path, files in sftp_walk(sftp,"."):
        for file in files:
            remote_file = os.path.join(os.path.join(path, file))
            ignore, reason = ignore_file(remote_file)
            if ignore:
                size=0
                status="OK"
                ignored=1
            else:
                try:
                    size=sftp.stat(remote_file).st_size
                    status = "OK"
                    ignored = 0
                except:
                    logging.exception("Error getting size: %s"%remote_file)
                    size = 0
                    status = "ERROR"
                    ignored = 1
            file_list.append(dict(id=username,path=path,file=file,file_path=remote_file,size=size, status=status, ignored=ignored, ignore_reason=reason))

    sftp.close()
    ssh.close()
    return file_list

def process(tablename, processedname, host, password, target_directory, size_limit, filespath):
    df = pd.read_csv(tablename)
    df_files = pd.DataFrame(columns=["id", "Name", "path", "file", "file_path", "size", "status", "ignored", "ignore_reason"])

    for index, row in df.iterrows():
        username = row.id
        try:
            dir = row.directory
        except:
            dir = None
        if dir in [None,"",float("nan")] or type(dir)!=type(""):
            print ("dir?",dir)
            dir = row.Name.replace("!","").replace("(","").replace(")","").replace("/","-")
        path = os.path.join(target_directory,dir)
        df.loc[index,"path"]=path
        df.loc[index,"directory"]=dir
        try:
            status = row.download_status
        except:
            status = "?"
        if status != "OK":
            print (index,username,dir)
            try:
                count,size,file_list = sftp_download(host, username, password=password, target_directory=path, size_limit=size_limit)
                dff = pd.DataFrame(file_list)
                dff.loc[:, "Name"]=row.Name
                df_files = df_files.append(dff, ignore_index=True)
                df_files.to_csv(filespath)

                df.loc[index,"file_count"]=int(count)
                df.loc[index, "size"] = int(size)
                df.loc[index, "download_status"] = "OK"
                print ("    OK")

            except:
                logging.exception("Download error : %s (id=%s)"%(row.Name,username))
                df.loc[index, "download_status"] = "Error"
                print ("    ERROR")
        df.to_csv(processedname,index=False)

def fetch_list(tablename, processedname, host, password,filespath):
    df = pd.read_csv(tablename)
    df_files = pd.DataFrame(columns=["id", "Name", "path", "file", "file_path", "size", "status", "ignored", "ignore_reason"])
    for index, row in df.iterrows():
        username = row.id
        try:
            status = row.download_status
        except:
            status = "?"
        if status != "OK":
            print (index,username)
            try:
                files = sftp_list(host, username, password=password)
                dff = pd.DataFrame(files)
                dff.loc[:, "Name"]=row.Name

                df_files = df_files.append(dff, ignore_index=True)
                df_files.to_csv(filespath)
                df.loc[index, "file_count"]=len(files)
                df.loc[index, "size"] = sum(x["size"] for x in files)
                df.loc[index, "download_status"] = "?"
                print ("    OK")
            except:
                logging.exception("Fetch list error : %s (id=%s)"%(row.Name,username))
                df.loc[index, "download_status"] = "Error"
                print ("    ERROR")
                traceback.print_exc()

        df.to_csv(processedname, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table", help="Table of scrapers_ in csv format", default="table.csv")
    parser.add_argument("--host", default='ds-ec2.scraperwiki.com', help="Host address")
    parser.add_argument("--target", default='scrapers', help="Target directory")
    parser.add_argument("--filelist", default='files.csv', help="Table with file names")
    parser.add_argument("--scan", action='store_true', help="Scan files only and produce a list of files")
    parser.add_argument("--size-limit", default=0, type=int, help="Size limit (in bytes) for individual downloaded files")
    parser.add_argument("--ignore-folders", default='ignore_folders.txt', help="File with newline-separated list of folders to ignore on remote server")
    parser.add_argument("--ignore-patterns", default='ignore_patterns.txt', help="File with newline-separated list of file patterns to ignore on remote server")
    parser.add_argument("-p", "--password", help="Password", default=None)
    parser.add_argument("-l", "--log", help="Log file", default=None)
    parser.add_argument("-d", "--debug", action='store_true', help="Log debug messages.")
    parser.add_argument("-v", "--verbose", action='store_true', help="Increase verbosity.")
    parser.add_argument("--processed", default="processed.csv", help="Store table of processed entries to a csv file.")

    config = parser.parse_args()

    log_level = logging.WARNING
    if config.verbose:
        log_level = min(log_level, logging.INFO)
    if config.debug:
        log_level = min(log_level, logging.DEBUG)

    if config.log is not None:
        logging.basicConfig(filename=config.log, level=log_level)

    try:
        ignore_folders = open(config.ignore_folders).read().split("/n")
        logging.info("Ignore folders loaded from %s:\n%s"%(config.ignore_folders,",\n".join(ignore_folders)))
    except:
        logging.error("Can't load ignore folders from %s"%(config.ignore_folders))
        logging.info("Using builtin ignore folders:\n%s"%(",\n".join(ignore_folders)))

    try:
        ignore_patterns = open(config.ignore_patterns).read().split("/n")
        logging.info("Ignore patterns loaded from %s:\n%s"%(config.ignore_patterns,",\n".join(ignore_patterns)))
    except:
        logging.error("Can't load ignore patterns from %s"%(config.ignore_patterns))
        logging.info("Using builtin ignore patterns:\n%s"%(",\n".join(ignore_patterns)))

    if config.scan:
        logging.info("Scanning started")
        fetch_list(config.table,config.processed, config.host, config.password, config.filelist)
        logging.info("Scanning finished")
    else:
        logging.info("Processing started")
        process(config.table,config.processed, config.host, config.password, config.target, config.size_limit, config.filelist)
        logging.info("Processing finished")

    #load_sftp_test1()