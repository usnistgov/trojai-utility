import unittest
import data_downloader
import os

class TestDataDownloader(unittest.TestCase):
    
    dataset_folder = 'downloaded_datesets/round6-train-dataset'

    def test_metadata_in_download_folder(self):
        filenames = os.listdir(self.dataset_folder)
        self.assertTrue('METADATA.csv' in filenames)

    def test_models_in_download_folder(self):
        filenames = os.listdir(self.dataset_folder)
        self.assertTrue('models' in filenames)

    def setUp(self):
        data_downloader.download_trojai_dataset(6, 'train')

if __name__ == '__main__':
    unittest.main(verbosity=3)