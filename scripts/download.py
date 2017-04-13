import os
import argparse

from audio_set import parse_csv, download_records

DOWNLOADER = "~/.local/bin/youtube-dl"
DOWNLOADER = os.path.expanduser(DOWNLOADER)
FFMPEG = 'ffmpeg'


def main(csv_file, out_dir, log_file, n_workers):
    records = parse_csv(csv_file)
    download_records(records, out_dir, log_file, n_workers, DOWNLOADER, FFMPEG)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-file', type=str, help='CSV file to parse',
                        default='./balanced_train_segments.csv')
    parser.add_argument('--out-dir', type=str, help='Output directory',
                        default='data/')
    parser.add_argument('--log-file', type=str, help='Log file name',
                        default='log')
    parser.add_argument('--n-workers', type=int, default=1,
                        help='Number of parallel workers')
    args = parser.parse_args()
    main(**args.__dict__)

