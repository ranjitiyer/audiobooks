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

import io

from src.loader.exporter import S3Exporter

TWO_DIGITS = '.*(\d\d).*'
HTML_TAGS = r'<[^>]+>'

TAG_RE = re.compile(HTML_TAGS)
section_num_pattern = re.compile(TWO_DIGITS)

exporter = S3Exporter()

def log_msg(msg: str, error: bool):
    file_name = 'info.log'
    if error:
        file_name = 'error.log'
    with io.open(file_name, 'a') as f:
        f.write(msg + '\n')

def log_status(msg: str):
    file_name = 'status.log'
    with io.open(file_name, 'a') as f:
        f.write(msg + '\n')


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
    try:
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
        print('{} is not a valid Librivox book'.format(title))
    except JSONDecodeError as e:
        print('Invalid JSON response for book {}'.format(title))
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
                pass
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
        authors.append(full_name.lower())
    return authors

# TODO
def normalize_genres(book):
    genres = []
    for genre in book.genres.getchildren():
        if genre['name']:
            genres.append(str(genre['name']).lower())
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
            #print("Starting {} ".format(book.id))
            ia_id = find_ia_id_from_title(book.title)
            if ia_id is None:
                log_msg("{} - {}".format(book.id, book.title), True)
            else:
                sections = get_sections(ia_id)
                if len(sections) > 0:
                    authors = normalize_authors(book)
                    genres = normalize_genres(book)
                    book_json = {
                        'id': int(book.id),
                        'title': str(book.title.text).lower(),
                        'description': TAG_RE.sub('', str(book.description.text)),
                        'num_sections': int(book.num_sections),
                        'sections': sections,
                        'authors': authors,
                        'genres': genres,
                        'totaltime': str(book.totaltime)
                    }
                    resultQ.put(json.dumps(book_json))
                else:
                    log_msg("{} - {}".format(book.id, book.title), True)
        workQ.task_done()

def get_start_offsets(total_books, limit_per_call, start_offset):
    offsets = [((limit_per_call * x) + start_offset) for x in
                    range(0, int(total_books / limit_per_call))]
    return offsets

def get_limits(total_books, limit_per_call):
    return [limit_per_call for _ in range(0, int(total_books / limit_per_call))]


def debug_title_sections(title):
    ia_id = find_ia_id_from_title(title)
    sections = get_sections(ia_id)
    print(sections)

def init():
    import os

    json_files = filter(lambda file_name: file_name.endswith('json'),
                        os.listdir('.'), )

    log_files = filter(lambda file_name: file_name.endswith('log'),
                        os.listdir('.'), )
    for file in json_files:
        print('Deleting file {}'.format(file))
        os.unlink(file)

    for file in log_files:
        print('Deleting file {}'.format(file))
        os.unlink(file)

if __name__ == '__main__':
    init()
    parser = argparse.ArgumentParser(description='Creates a JSON database of '
                                                 'book downloaded from Librivox '
                                                 'and uploads them to S3')

    parser.add_argument('--offset', help='starting offset when searching '
                                         'librivox', type=int, default=0)
    parser.add_argument('--limit', help='number of records to obtain in each '
                                        'request', type=int, default=10)
    parser.add_argument('--numbooks',
                        help='number of books to preocess to obtain in each '
                             'request',
                        type=int, default=10)
    parser.add_argument('--threads', help='number of threads to use in the '
                                          'processing', type=int, default=2)
    parser.add_argument('--dryrun', help='write to local file, don\'t '
                                         'upload to s3', action='store_true')

    parsed_args = parser.parse_args()

    # debug_title_sections('Odyssey')
    # debug_title_sections('Canterville Ghost')
    # debug_title_sections('Uncle Tom\'s Cabin')
    # debug_title_sections('Adventures of Huckleberry Finn')

    print(parsed_args)

    # limit is fixed
    limit = parsed_args.limit
    offset = parsed_args.offset
    total_books = parsed_args.numbooks
    num_threads = parsed_args.threads

    # process in batches of 10
    offsets = get_start_offsets(total_books, limit, offset)
    limits = get_limits(total_books, limit)
    for offset_limit in zip(offsets, limits):
        workQ = Queue()
        resultQ = Queue()
        threads = []

        offset = offset_limit[0]
        limit = offset_limit[1]

        print("Starting processing offset {} (limit {})".format(offset, limit))

        # get books
        books = get_books(limit, offset)

        for i in range(num_threads):
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)

        for book in books.getchildren():
            log_msg("{} - {}".format(book.id, book.title), False)
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

        if not parsed_args.dryrun:
            print("Writing to S3")
            exporter.export(books_json)
        else:
            with io.open("books{}.json".format(offset), 'w') as f:
                f.write(books_json)

        log_status("Completed processing offset {} (limit {})".format(offset, limit))