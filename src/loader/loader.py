from json import JSONDecodeError
from urllib.parse import ParseResult

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

from lxml.objectify import StringElement

from src.loader.exporter import S3Exporter

HTML_TAGS = r'<[^>]+>'
TAG_RE = re.compile(HTML_TAGS)

TWO_DIGITS = '.*(\d\d).*'
section_num_pattern = re.compile(TWO_DIGITS)

CHAPTER_NUMBER = 'Chapter (\d?\d)'
chapter_num_pattern = re.compile(CHAPTER_NUMBER)

CHAPTERS_NUMBER = 'Chapters (\d?\d)'
chapters_num_pattern = re.compile(CHAPTER_NUMBER)

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


# url_iarchive
# http://www.archive.org/details/being_earnest_librivox

def find_ia_id_from_title(book):
    title = book.title

    # check if its available in librivox meta-data
    from urllib.parse import urlsplit
    url = book.url_iarchive
    if url:
        parse_result: ParseResult = urlsplit(url.text)
        ia_id = parse_result.path.rsplit('/')[-1]
        return ia_id
    else:
        # get it the hard way
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
            raise Exception("Error processing book {} ".format(title))
        except JSONDecodeError as e:
            print('Invalid JSON response for book {}'.format(title))
            raise e

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
        #print(response.content)
        print(e)

# Build section mp3 dict
def get_sections(ia_id):
    # find section mp3 names
    sections = {}
    try:
        # get file server base path
        path = get_ia_file_server_base_path(ia_id)

        if path is None:
            return sections

        url = "http://archive.org/metadata/{}/files".format(ia_id)
        response = requests.get(url)
        _json = json.loads(response.content)
        results = _json['result']

        default_section_number = 0
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
                # Examples:
                # 11 - Chapter XI: The Tweedles
                # 01 - Chapter I: Peggy the Pig
                # Chater 1: - Chapter XI: The Tweedles
                # Chapters 1:- Chapter I: Peggy the Pig
                match = section_num_pattern.match(section_name)
                if match is None:
                    match = chapter_num_pattern.match(section_name)
                    if match is None:
                        match = chapters_num_pattern.match(section_name)
                if match:
                    sections[int(match.group(1))] = {
                        'number': int(match.group(1)),
                        'name': section_name,
                        'url': abs_url.replace("http", "https")
                    }
                else:
                    sections[str(default_section_number)] = {
                        'number': default_section_number,
                        'name': section_name,
                        'url': abs_url.replace("http", "https")
                    }
                    default_section_number += 1
    except JSONDecodeError as e:
        print(e)
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

def get_start_offsets(total_books, limit_per_call, start_offset):
    offsets = [((limit_per_call * x) + start_offset) for x in
                    range(0, int(total_books / limit_per_call))]
    return offsets

def get_limits(total_books, limit_per_call):
    return [limit_per_call for _ in range(0, int(total_books / limit_per_call))]


def debug_title_sections(id):
    book = get_book_metadata(id)
    ia_id = find_ia_id_from_title(book)
    sections = get_sections(ia_id)
    print(sections)

def process(book):
    print(book)
    if book.language == 'English':
        ia_id = find_ia_id_from_title(book)
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
                print("Returning {} ".format(json.dumps(book_json)))
                log_msg("{} - {}".format(book.id, book.title), False)
                return json.dumps(book_json)
            else:
                log_msg("{} - {}".format(book.id, book.title), True)
                raise Exception("Error processing book {} ".format(book.id))

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
    print(parsed_args)

    # debug_title_sections('Book of Tea')
    #debug_title_sections('Canterville Ghost')
    #debug_title_sections('Emma')
    #debug_title_sections(230)

    # ia_id = get_ia_id_from_book_id(86)
    # sections = get_sections(ia_id)
    # print(sections)

    # limit is fixed
    limit = parsed_args.limit
    offset = parsed_args.offset
    total_books = parsed_args.numbooks
    num_threads = parsed_args.threads

    # process in batches of 10
    offsets = get_start_offsets(total_books, limit, offset)
    limits = get_limits(total_books, limit)

    from concurrent import futures
    pool = futures.ThreadPoolExecutor(max_workers=num_threads)

    books_json = ''
    for offset_limit in zip(offsets, limits):
        offset = offset_limit[0]
        limit = offset_limit[1]
        print("Starting processing offset {} (limit {})".format(offset, limit))

        # get books
        books = get_books(limit, offset)

        # process all books
        all_futures = []
        for book in books.getchildren():
            future = pool.submit(process, book)
            all_futures.append(future)

        print('Number of futures submitted {}'.format(len(all_futures)))
        for f in all_futures:
            try:
                if f.result() is not None:
                    books_json += f.result() + '\n'
                else:
                    print("Process task returned None")
            except Exception as exc:
                print(exc)

        log_status("Completed processing offset {} (limit {})".format(offset, limit))

    books_json = books_json.rstrip()

    if not parsed_args.dryrun:
        print("Writing to S3")
        exporter.export(books_json)
    else:
        with io.open("books{}.json".format(offset), 'w') as f:
            f.write(books_json)


    # shutdown pool
    pool.shutdown()