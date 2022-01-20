import os
import shutil
import tarfile
from tqdm import tqdm
from pathlib import Path
import requests
import re
from shutil import move
from google_drive_downloader import GoogleDriveDownloader as gdd
from constants import round_split_to_gdrive_file_id, round_split_to_nist_id


def download_trojai_dataset(round_number, split, destination_folder=None):
    check_that_split_is_supported(split)
    check_that_round_number_is_supported(round_number)

    destination_folder = substitute_destination_folder_if_its_none(destination_folder)
    make_folder_if_necessary(destination_folder)

    dataset_folder = get_dataset_folder(destination_folder, round_number, split)
    is_already_downloaded = check_if_data_is_already_downloaded(dataset_folder)
    if is_already_downloaded:
        print(f'data is already downloaded at {destination_folder}')
        return True

    else:
        try:
            print('downloading with gdrive')
            download_with_gdrive(round_number, split, destination_folder, dataset_folder)
        except:
            print('failed to download with gdrive. downloading directly')
            get_data_from_nist_website(round_number, split, dataset_folder)
        finally:
            print('data downloaded')



def check_that_split_is_supported(split):
    supported_splits = ['train', 'test', 'holdout']
    assert split in supported_splits

def check_that_round_number_is_supported(round_number):
    # 99 is used for testing. see test.py
    supported_rounds = [5,6,7,8, 99]
    assert round_number in supported_rounds

def substitute_destination_folder_if_its_none(destination_folder):
    if destination_folder is None:
        current_dir = os.path.dirname(__file__)
        destination_folder = os.path.join(current_dir, 'downloaded_datesets')
    return destination_folder

def make_folder_if_necessary(folder):
    if not os.path.isdir(folder):
        os.mkdir(folder)

def get_dataset_folder(destination_folder, round_number, split):
    return os.path.join(destination_folder, f'round{round_number}-{split}-dataset')

def check_if_data_is_already_downloaded(dataset_folder):
    if os.path.isdir(dataset_folder):
        filenames = os.listdir(dataset_folder)
        has_metadata = 'METADATA.csv' in filenames
        has_models = 'models' in filenames
        if has_metadata and has_models:
            return True
        else:
            shutil.rmtree(dataset_folder)
            return False
    return False

def download_with_gdrive(round_number, split, destination_folder, dataset_folder):
    gdd.download_file_from_google_drive(file_id=round_split_to_gdrive_file_id[(round_number, split)],
                            dest_path=destination_folder,
                            showsize=True)
    tar_path = f'{dataset_folder}.tar.gz'
    tf = tarfile.open(tar_path)
    tf.extractall(path=destination_folder)
    os.remove(tar_path)
    return True


def get_data_from_nist_website(round_number, split, dataset_folder):
    make_folder_if_necessary(dataset_folder)

    website = f'https://data.nist.gov/od/id/{round_split_to_nist_id[(round_number, split)]}'
    urls = get_urls_from_site(website)

    download_all_urls(urls, out_dir=dataset_folder)

    filepaths_to_unzip = get_filepaths_to_unzip(dataset_folder)
    unzip_all_files(filepaths_to_unzip)

    move_all_files_up_if_necessary(urls, dataset_folder)

    models_folder = os.path.join(dataset_folder, 'models')
    make_folder_if_necessary(models_folder)
    move_all_models_to_models_folder(dataset_folder, models_folder)
    return True

def get_urls_from_site(website):
    req = requests.get(website)
    all_urls = re.findall(r'"downloadURL": "(\S+)"', req.text)
    urls = [url for url in all_urls if not url.endswith('.sha256')]
    return urls


def get_round_json_filepath(round_number, split):
    this_scripts_path = Path(os.path.abspath(__file__))
    this_scripts_parent_filepath = os.path.dirname(this_scripts_path)

    second_part = f'data_repo_json_files/round{round_number}-{split}.json'
    return os.path.join(this_scripts_parent_filepath, second_part)

def get_all_urls(round_json):
    urls = []
    for entry in round_json['ResultData'][0]['components']:
        if check_if_entry_is_not_sha(entry) and check_url_is_not_empty(entry):
            urls.append(entry['downloadURL'])
    return urls

def check_if_entry_is_not_sha(entry):
    has_downloadURL = 'downloadURL' in entry 
    ends_in_tar_gz = False
    if has_downloadURL:
        contains_sha_check = entry['downloadURL'].endswith('.sha256') 
    return has_downloadURL and not contains_sha_check

def check_url_is_not_empty(entry):
    return entry['downloadURL'] != ""
    

def download_all_urls(urls, out_dir):
    for url_num, single_url in tqdm(enumerate(urls), total=len(urls), desc='overall progress'):
        request_filename = make_url_filename_from_dir(out_dir, single_url)
        response = requests.get(single_url, stream=True)
        write_chunks_from_response(response, request_filename, url_num)


def make_url_filename_from_dir(out_dir, single_url):
    return os.path.join(out_dir, os.path.basename(single_url))


def write_chunks_from_response(response, out, url_num):
    chunk_size=1024
    with open(out, 'wb') as fd:
        total_size_in_bytes= int(response.headers.get('content-length', 0))
        progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True, 
                                        leave=False, desc=f'\tprogress on url {url_num+1}')
        for chunk in response.iter_content(chunk_size=chunk_size):
            progress_bar.update(len(chunk))
            fd.write(chunk)

def move_all_files_up_if_necessary(urls, dataset_folder):
    parent_dirname = os.listdir(dataset_folder)[0]
    if len(urls) == 1 and 'round' in parent_dirname:
        parent_dirpath = os.path.join(dataset_folder, parent_dirname)
        filenames_to_move = os.listdir(parent_dirpath)
        for filename in filenames_to_move:
            filepath = os.path.join(parent_dirpath, filename)
            shutil.move(filepath, dataset_folder)
        os.rmdir(parent_dirpath)

def get_filepaths_to_unzip(round_destination_folder):
    files_to_unzip = []
    for f in os.listdir(round_destination_folder):
        if f.endswith('tar.gz'):
            files_to_unzip.append(os.path.join(round_destination_folder, f))
    return files_to_unzip


def unzip_all_files(filepaths_to_unzip):
    for filepath in filepaths_to_unzip:
        tf = tarfile.open(filepath, "r:gz")
        tf.extractall(os.path.dirname(filepath))
        os.remove(filepath)

def move_all_models_to_models_folder(source, destination):
    for f in os.listdir(source):
        if os.path.isdir(f) and f.startswith('id-'):
            move(os.path.join(source, f), destination)

