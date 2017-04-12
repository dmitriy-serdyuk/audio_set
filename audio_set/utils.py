from __future__ import print_function
import csv
import os
import random
import subprocess
import time
import youtube_dl
from six.moves.queue import Queue
from threading import Thread
from youtube_dl.postprocessor.ffmpeg import EXT_TO_OUT_FORMATS


class DownloadThread(Thread):
    def __init__(self, queue, out_dir, download_func, log_file):
        super(DownloadThread, self).__init__()
        self.queue = queue
        self.out_dir = out_dir
        self.daemon = True
        self.download_func = download_func
        self.log_file = log_file

    def run(self):
        while True:
            url, start, end, _ = self.queue.get()
            try:
                self.download_func(url, start, end, self.out_dir)
            except Exception as e:
                self.log_file.write('failed {} {}\n'.format(
                    url, unicode(e.message).encode("utf-8")))
                self.log_file.flush()
            self.queue.task_done()


def run_subprocess(args):
    proc = subprocess.Popen(args, stdout=subprocess.PIPE)
    outs = []
    while True:
        line = proc.stdout.readline()
        if line != '':
            outs.append(line.rstrip())
        else:
            break
    return outs


def format_time(seconds):
    m, s = divmod(float(seconds), 60)
    return "00:%02d:%02d" % (m, s)


class DownloadAudio(object):
    def __init__(self, ydl, ffmpeg):
        self.ydl = ydl
        self.ffmpeg = ffmpeg

    def __call__(url, start, end, out_dir, *args, **kwargs):
        if (url + '.ogg') in os.listdir(out_dir):
            print('.. skip download')
            return
        print('.. download {}: from {} to {}'.format(url, start, end))
        duration = format_time(float(end) - float(start))
        start = format_time(start)
        info = self.ydl.extract_info(url, download=False)
        download_url = info['url']
        format_ = EXT_TO_OUT_FORMATS.get(info['ext'], info['ext'])

        out = run_subprocess(
            [self.ffmpeg, "-y", "-i", download_url, "-c", "copy", "-f", 
             format_, "-ss", start, "-t", duration, 
             "file:{}{}.ogg".format(out_dir, url)])
        print(out)


def parse_scv(csv_file):
    records = []
    print('.. reading csv {}'.format(csv_file))
    with open(csv_file, 'rb') as f:
        csv_file = csv.reader(
            f, quotechar='"', doublequote=False, 
            quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in csv_file:
            if row[0].startswith('#'):
                continue
            records.append(row)
    return records


def download_loop(records, out_dir, n_workers=1, downloader=None, 
                  ffmpeg=None):
    if not downloader:
        downloader = 'youtube-dl'
    if not ffmpeg:
        ffmpeg = 'ffmpeg'
    ydl = youtube_dl.YoutubeDL(
        {'outtmpl': '%(id)s%(ext)s', 'format': 'bestaudio'})
    print('.. downloading')
    random.shuffle(records)
    download_queue = Queue()
    for record in records:
        download_queue.put(record)
    audio_downloader = DownloadAudio(ydl, ffmpeg)
    with open(log_file, 'w') as log_file:
        for i in n_workers:
            thread = DownloadThread(
                download_queue, out_dir, audio_downloader, log_file)
            thread.start()
        download_queue.join()

