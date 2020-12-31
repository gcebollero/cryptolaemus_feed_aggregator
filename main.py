import requests
import xml.etree.ElementTree as ET
import re
import os
import csv
import pandas as pd
import glob


def extract_ips(published: str, content:str):
    with open('IP/{}.csv'.format(published),'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['IP', 'PORT'])
        for ip in re.findall(r"([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:?[0-9]{0,5})",content):
            ip = ip.split(':')
            if len(ip) == 1:
                csv_writer.writerow([ip[0], ''])
            else:
                csv_writer.writerow([ip[0], ip[1]])


def extract_sha256s(published:str, content:str):
    with open('SHA256/{}.csv'.format(published),'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['SHA256'])
        for sha in re.findall(r'[a-fA-F0-9]{64}', content):
            csv_writer.writerow([sha])


def extract_urls(published:str, content:str):
    with open('URL/{}.csv'.format(published),'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['URL'])
        for url in re.findall(r'http:\/\/\S+', content):
            csv_writer.writerow([url])

def download():
    feed = requests.get('https://paste.cryptolaemus.com/feed.xml')
    root = ET.fromstring(feed.text)
    i = 0
    last_published = None
    last_content = None
    for section in root.iterfind('.//'):
        if 'published' in section.tag:
            last_published = section.text
        if 'content' in section.tag:
            last_content = section.text
        if last_published is not None and last_content is not None:
            extract_ips(last_published, last_content)
            extract_sha256s(last_published, last_content)
            extract_urls(last_published, last_content)
            last_published = None
            last_content = None


def check_folders():
    if not os.path.exists('IP'):
        os.mkdir('IP')
    if not os.path.exists('URL'):
        os.mkdir('URL')
    if not os.path.exists('SHA256'):
        os.mkdir('SHA256')


def aggregate():
    for report in ('IP','SHA256','URL'):
        data_frame = pd.DataFrame()
        # Last 30 feeds order by date
        for file in sorted(glob.glob("{}/*.csv".format(report)), reverse=True)[0:30]:
            tmp = pd.read_csv(file)
            data_frame = pd.concat([data_frame, tmp])
            data_frame.drop_duplicates(subset=report, inplace=True)
            data_frame.dropna(inplace=True)
        if report == "IP":
            data_frame['PORT'] = data_frame['PORT'].astype(int)
        data_frame.to_csv('{}.csv'.format(report), index=False)


if __name__ == '__main__':
    check_folders()
    download()
    aggregate()

