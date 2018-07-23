from paramiko import SSHClient
from scp import SCPClient
import pandas as pd
import argparse
import logging
from stat import S_ISDIR
import os

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

def sftp_download(host, username, password=None, target_directory="."):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    if password is None:
        ssh.connect(host,username=username)
    else:
        ssh.connect(host, username=username,password=password)
    sftp = ssh.open_sftp()

    def ignore(path):
        return ".cache" in path or ".git" in path or "/venv/" in path or "__pycache__" in path
    def sftp_walk(remotepath):
        if not ignore(remotepath):
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
                for x in sftp_walk(new_path):
                    yield x
    count = 0
    size = 0
    for path, files in sftp_walk("."):
        for file in files:
            # sftp.get(remote, local) line for dowloading.
            localpath = os.path.join(target_directory,path)
            try:
                os.makedirs(localpath)
            except:
                pass
            sftp.get(os.path.join(os.path.join(path, file)), os.path.join(localpath,file))
            size+=sftp.stat(os.path.join(os.path.join(path, file))).st_size
            count+=1

    sftp.close()
    ssh.close()
    return count, size

def process(tablename, processedname, host, password, target_directory):
    df = pd.read_csv(tablename)
    for index, row in df.iterrows():
        username = row.id
        name = row.Name.replace("!","").replace("(","").replace(")","")
        path = os.path.join(target_directory,name)
        df.loc[index,"path"]=path
        print (username,name)
        try:
            count,size = sftp_download(host, username, password=password, target_directory=path)
            df.loc[index,"file_count"]=count
            df.loc[index, "size"] = size
            df.loc[index, "download_status"] = "OK"
        except:
            logging.exception("Download error : %s (id=%s)"%(row.Name,username))
            df.loc[index, "download_status"] = "Error"
    df.to_csv(processedname,index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table", help="Table of scrapers in csv format", default="table.csv")
    parser.add_argument("--host", default='ds-ec2.scraperwiki.com', help="Host address")
    parser.add_argument(      "--target", default='scrapers', help="Target directory")
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

    logging.info("Processing started")
    process(config.table,config.processed, config.host, config.password, config.target)
    logging.info("Processing finished")

    #load_sftp_test1()