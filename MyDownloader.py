"""
Developer: Mitesh H. Budhabhatti
Email: mitesh.budhabhatti@gmail.com
"""

import os
import requests
import threading
import sys
import argparse


def get_url_info(url):
    resp = requests.get(url, stream=True)
    filename = url.split('/')[-1]
    filelen = int(resp.headers['Content-Length'])

    resp.close()
    if 'Accept-Ranges' in resp.headers:
        # Just to see whether server supports partial download (i.e. chunks)
        headers = {'Range': 'bytes={0}-{1}'.format(0, 512)}
        resp = requests.get(url, headers=headers, stream=True)
        supportchunks = resp.status_code == requests.codes.partial_content
        resp.close()
    else:
        supportchunks = False

    return (filename, filelen, supportchunks)


def download_chunk(url, filename, targetdir, chunkno, start, end):
    headers = {'Range': 'bytes={0}-{1}'.format(start, end)}
    resp = requests.get(url, headers=headers, stream=True)

    chunksize = 10 * 1024

    tempfile = os.path.join(targetdir, '{0}.c{1}'.format(filename, chunkno))

    if resp.status_code == requests.codes.partial_content:
        mode = 'ba'
    else:
        mode = 'bw'

    with open(tempfile, mode=mode) as chunkfile:
        for chunk in resp.iter_content(chunksize):
            chunkfile.write(chunk)
            chunkfile.flush()

    chunkfile.close()
    resp.close()


def merge_chunks(filename, targetdir, chunks):
    target_filepath = os.path.join(targetdir, filename)
    with open(target_filepath, 'wb') as target:
        for i in range(1, chunks + 1):
            source_filepath = os.path.join(targetdir, '{0}.c{1}'.format(filename, i))
            with open(source_filepath, 'rb') as source:
                target.write(source.read())
            os.remove(source_filepath)


def download(url, targetdir, threads):
    (filename, filelen, supportchunks) = get_url_info(url)

    if supportchunks:
        noofchunks = threads
    else:
        noofchunks = 1

    print('File {0} will be downloaded in {1} threads.'.format(filename, noofchunks))

    chunksize = filelen // noofchunks
    lastchunksize = filelen % noofchunks

    # Because start is end + 1.  For the first chunk start has to be 0
    end = -1
    chunk_threads = []
    for i in range(1, noofchunks + 1):
        chunkno = i
        start = end + 1
        if i == noofchunks:
            # In the last chunk, consider remaining size also
            end = ((chunksize * (noofchunks - 1)) + (chunksize + lastchunksize)) - 1
        else:
            end = start + (chunksize - 1)

        chunkfilepath = os.path.join(targetdir, '{0}.c{1}'.format(filename, chunkno))

        # If the file exist and chunk download is supported then only we can resume from where it was left
        if os.path.exists(chunkfilepath) and supportchunks:
            if os.path.getsize(chunkfilepath) == (end - start + 1):
                # If file is fully downloaded, continue with the next chunk
                print('Chunk file for the chunk {0} found.  No need to download the chunk again.'.format(chunkno))
                continue
            else:
                start = os.path.getsize(chunkfilepath)  # - 1

        th = threading.Thread(target=download_chunk, args=(url, filename, targetdir, chunkno, start, end),
                              name='Thread_{0}_C{1}'.format(filename, chunkno))
        th.daemon = True
        th.start()
        chunk_threads.append(th)

        # print('{0} to {1}'.format(start, end))

    for th in chunk_threads:
        th.join()

    print('All the chunks downloaded.  Finishing..')
    merge_chunks(filename, targetdir, noofchunks)
    print('Download complete !!')


if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Download the file in multiple chunks, each chunk in a separate thread.')
    aparser.add_argument('url', type=str, help='URL from where file is to be downloaded.')
    aparser.add_argument('--path', '-p', type=str, help='Path where chunk files will be created and also where the actual file will be downloaded. Default is current directory.', default=os.getcwd())
    aparser.add_argument('--threads', '-t', type=int, help='Number of chunks to create.', default=50)

    args = aparser.parse_args()

    url = args.url
    targetdir = args.path
    threads = args.threads

    if len(sys.argv) == 3:
        targetdir = sys.argv[2]
    else:
        targetdir = os.getcwd()

    if not targetdir.endswith('\\'):
        targetdir = targetdir + '\\'

    download(url, targetdir, threads)
