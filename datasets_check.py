from hdx.data.dataset import Dataset
from hdx.facades.simple import facade
from hdx.utilities import is_valid_uuid
import pandas as pd
from os.path import join, expanduser
import urllib.parse as up

def main():
    dfsw = pd.read_csv("table.csv")
    swid_to_name = dict(dfsw.loc[:, ["id", "Name"]].as_matrix())


    datasets = Dataset.get_all_datasets()
    data=[]

    for i,dataset in enumerate(datasets):
        name = dataset['name']
        print (i,name)
        maintainer = dataset['maintainer']
        org=""
        if not is_valid_uuid(maintainer):
            org = dataset.get('organization')
            if org:
                org = org['title']
            else:
                org = 'NONE!'
            if not maintainer:
                maintainer = 'NONE!'
        resources = Dataset.get_all_resources([dataset])
        resource_name = ""
        url=""
        scraperwiki_in_url=False
        scraperwiki_id = ""
        scraperwiki_hyperlink = ""
        for resource in resources:
            url = resource["url"]
            scraperwiki_in_url = "scraperwiki" in url
            resource_name = resource["name"]
            if scraperwiki_in_url:
                scraperwiki_id = next(filter(len,up.urlparse(url).path.split("/")))
                scraperwiki_hyperlink = '=HYPERLINK("https://app.quickcode.io/dataset/%s", "%s")' % (scraperwiki_id, scraperwiki_id)
                break
            else:
                scraperwiki_id = ""
                scraperwiki_hyperlink = ""
        dataset_hyperlink = '=HYPERLINK("https://data.humdata.org/dataset/%s", "%s")'%(name,name)
        data.append(dict(
            dataset_name=name,
            dataset_hyperlink = dataset_hyperlink,
            resource_name=resource_name,
            maintainer=maintainer,
            resource_url=url,
            scraperwiki_id = scraperwiki_id,
            scraperwiki_name = swid_to_name.get(scraperwiki_id,""),
            scraperwiki_hyperlink=scraperwiki_hyperlink,
            scraperwiki_in_url=scraperwiki_in_url))
    df=pd.DataFrame(data)
    df.to_csv("dataset.csv",index=False)
    writer = pd.ExcelWriter("dataset.xlsx")
    df.to_excel(writer)
    writer.save()
    writer.close()

    with open("dataset.html","w") as f:
        f.write("""<html>
  <head>
    <style>
      table{width: 100%;}
      th {border-bottom: 1px solid #555;}
      tr:hover {background-color: #f5f5fb;}
      tr:nth-child(even) {background-color: #f2f2f2;} 
      tr:nth-child(even):hover {background-color: #f2f2fb;} 
    </style>
  </head>
  <body>
    <table >
      <tr><th>Dataset</th><th>Scraperwiki ID</th><th>Scraperwiki Name</th></tr>
""")
        for index,row in df[df.scraperwiki_in_url].iterrows():
            f.write("""
      <tr>
        <td><a href='https://data.humdata.org/dataset/%(dataset_name)s'>%(dataset_name)s</a></td>
        <td>%(scraperwiki_id)s</td>
        <td><a href='https://app.quickcode.io/dataset/%(scraperwiki_id)s'>%(scraperwiki_name)s</a></td>
      </tr>
"""%dict(row))

        f.write("""</table></body></html>""")

if __name__ == '__main__':
    facade(main, hdx_site='prod', user_agent_config_yaml = join(expanduser('~'), '.dscheckuseragent.yml'))
