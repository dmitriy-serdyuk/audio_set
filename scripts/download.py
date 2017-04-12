import argparse
import os
import subprocess
import csv
import time
import pandas
import random
import youtube_dl
from threading import Thread
from youtube_dl.postprocessor.ffmpeg import EXT_TO_OUT_FORMATS
from six.moves.queue import Queue

DOWNLOADER = "~/.local/bin/youtube-dl"
DOWNLOADER = os.path.expanduser(DOWNLOADER)
DL_ARGS = ["-j", "-f", "bestaudio"]
FFMPEG = 'ffmpeg'


class DownloadThread(Thread):
    def __init__(self, queue, destfolder):
        super(DownloadThread, self).__init__()
        self.queue = queue
        self.destfolder = destfolder
        self.daemon = True

    def run(self):
        while True:
            url = self.queue.get()
            try:
                self.download_url(self.destfolder, url, self.ident)
            except Exception,e:
                print "   Error: %s"%e
            self.queue.task_done()

    def download_url(self, destfolder, url, ident):
        # change it to a different way if you require
        name = url.split('/')[-1]
        dest = os.path.join(destfolder, name)
        print("[%s] Downloading %s -> %s"%(ident, url, dest))
        urllib.urlretrieve(url, dest)


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


def main(csv_file, out_dir, log_file):
    filename = ''
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

    print('.. downloading')
    random.shuffle(records)
    with open(log_file, 'w') as log_file:
        for i, (url, start, end, _) in enumerate(records):
            try:
                download(url, start, end, out_dir)
            except Exception as e:
                log_file.write('failed {} {}\n'.format(url, unicode(e.message).encode("utf-8")))
            print('.. finished {}'.format(i))

ydl = youtube_dl.YoutubeDL({'outtmpl': '%(id)s%(ext)s', 'format': 'bestaudio'})


def download(url, start, end, out_dir, *args, **kwargs):
    if (url + '.ogg') in os.listdir(out_dir):
        print('.. skip download')
        return
    print('.. download {}: from {} to {}'.format(url, start, end))
    duration = format_time(float(end) - float(start))
    start = format_time(start)
    info = ydl.extract_info(url, download=False)
    download_url = info['url']
    format_ = EXT_TO_OUT_FORMATS.get(info['ext'], info['ext'])

    subprocess.call(
        [FFMPEG, "-y", "-i", download_url, "-c", "copy", "-f", format_,
        "-ss", start, "-t", duration, "file:{}{}.ogg".format(out_dir, url)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-file', type=str, help='CSV file to parse',
                        default='./balanced_train_segments.csv')
    parser.add_argument('--out-dir', type=str, help='Output directory',
                        default='data/')
    parser.add_argument('--log-file', type=str, help='Log file name',
                        default='log')
    args = parser.parse_args()
    main(**args.__dict__)

