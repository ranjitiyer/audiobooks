from json import JSONDecodeError

import requests
#from lxml import etree, objectify

import json
import copy
import re
import boto3
from lxml import objectify
from lxml import etree

import argparse
import sys
from queue import Queue
import threading

from src.loader.exporter import S3Exporter

TWO_DIGITS = '.*(\d\d).*'
HTML_TAGS = r'<[^>]+>'

TAG_RE = re.compile(HTML_TAGS)
section_num_pattern = re.compile(TWO_DIGITS)

exporter = S3Exporter()


def get_book_metadata(id):
    url = 'https://librivox.org/api/feed/audiobooks'
    payload = {'id': id, 'extended': 'true'}
    response = requests.get(url, params=payload)
    if response.status_code is 200:
        root = objectify.fromstring(response.content)
        return copy.deepcopy(root.books.book)


# get the title that ends in librivox
def is_librivox(doc):
    if doc['identifier'].endswith('librivox'):
        return True
    return False


def find_ia_id_from_title(title):
    _params = {'q': title, 'fl[]': 'identifier', 'page': 1, 'output': 'json'}
    url = 'https://archive.org/advancedsearch.php'
    response = requests.get(url, params=_params)
    _json = json.loads(response.content)
    docs = _json['response']['docs']
    lib_docs = list(filter(is_librivox, docs))
    if len(lib_docs) > 0:
        # Testing all potential librivox ids until a valid one is found
        for doc in lib_docs:
            if is_id_valid(doc['identifier']):
                return doc['identifier']
    return None


# Validate if ID is valid
# new_alice_in_the_old_wonderland_1603_librivox
def is_id_valid(ia_id):
    url = "http://archive.org/metadata/{}/metadata/title".format(ia_id)
    response = requests.get(url)
    if response.status_code is 200:
        _json = json.loads(response.content)
        if _json.get('error') is None:
            return True
        else:
            print(
                "Error getting metadata for id {} from internet archive".format(
                    ia_id))
            return False


# Get section mp3 folder path
def get_ia_file_server_base_path(ia_id):
    _params = {'output': 'json'}
    url = "https://archive.org/details/{}".format(ia_id)
    response = requests.get(url, params=_params)
    try:
        _json = json.loads(response.content)
        _server = _json['server']
        _dir = _json['dir']
        return {'server': _server, 'dir': _dir}
    except JSONDecodeError as e:
        print(response.content)
        print(e)

# Build section mp3 dict
def get_sections(ia_id):
    # get file server base path
    path = get_ia_file_server_base_path(ia_id)

    # find section mp3 names
    sections = {}
    url = "http://archive.org/metadata/{}/files".format(ia_id)
    response = requests.get(url)
    _json = json.loads(response.content)
    results = _json['result']
    for result in results:
        if result['name'].endswith('_64kb.mp3'):
            file_name = result['name']
            # section name
            if not result['title'] is None:
                section_name = result['title']
            else:
                section_name = file_name
            # section url
            abs_url = "http://{}{}/{}".format(path['server'], path['dir'],
                                              file_name)

            # extract section number from name
            # Ex: 11 - Chapter XI: The Tweedles
            # Ex: 01 - Chapter I: Peggy the Pig
            match = section_num_pattern.match(section_name)
            if match:
                sections[int(match.group(1))] = {
                    'number': int(match.group(1)),
                    'name': section_name,
                    'url': abs_url.replace("http", "https")
                }
            else:
                print("WARN: Unable to parse section number for {}".format(
                    section_name))
    return sections


def normalize_authors(book):
    authors = []
    for author in book.authors.getchildren():
        full_name = ''
        if author['first_name']:
            full_name += author['first_name'] + ' '
        if author['last_name']:
            full_name += author['last_name']
        # add full name
        authors.append(full_name)
    return authors


# TODO
def normalize_genres(book):
    genres = []
    for genre in book.genres.getchildren():
        if genre['name']:
            genres.append(str(genre['name']))
    return genres


def get_books(limit, offset):
    url = "https://librivox.org/api/feed/audiobooks?limit={}&offset={}&extended=true" \
        .format(limit, offset)
    response = requests.get(url)
    if response.status_code is 200:
        root = objectify.fromstring(response.content)
        return copy.deepcopy(root.books)


workQ = Queue()
resultQ = Queue()
threads = []


def worker():
    while True:
        book = workQ.get()
        if book is None:
            break
        if book.language == 'English':
            print("Starting {} ".format(book.id))
            ia_id = find_ia_id_from_title(book.title)
            sections = get_sections(ia_id)
            authors = normalize_authors(book)
            genres = normalize_genres(book)

            book_json = {
                'id': int(book.id),
                'title': str(book.title.text),
                'description': TAG_RE.sub('', str(book.description.text)),
                'num_sections': int(book.num_sections),
                'sections': sections,
                'authors': authors,
                'genres': genres,
                'totaltime': str(book.totaltime)
            }
            resultQ.put(json.dumps(book_json))
            print("Completed {} ".format(book.id))

        workQ.task_done()


limit = 100
offset = 0
num_threads = 2
if sys.argv[1]:
    limit = sys.argv[1]
if sys.argv[2]:
    offset = sys.argv[2]
if sys.argv[3]:
    num_threads = int(sys.argv[3])

# get all books
books = get_books(limit, offset)

print("got books")

for i in range(num_threads):
    t = threading.Thread(target=worker)
    t.start()
    threads.append(t)

print("created threads")

for book in books.getchildren():
    workQ.put(book)

# block until all done
workQ.join()

# stop workers
for i in range(num_threads):
    workQ.put(None)
for t in threads:
    t.join()

books_json = ''
while not resultQ.empty():
    books_json += resultQ.get() + '\n'
books_json = books_json.rstrip()

exporter.export(books_json)