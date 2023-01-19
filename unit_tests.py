import unittest
from os import listdir
from lib.utils import find_files_of_type

class UnitTestCase(unittest.TestCase):
    
    def test_source_file_exist(self, path = 'source' ): 
        filenames = listdir(path)
        file_list = len([filename for filename in filenames if filename.endswith('.parquet')])
        self.assertGreater(file_list, 1, 'Source Folder does not contain parquet files')
        
    def test_file_strucure(self):
        csv_file = find_files_of_type('./staging', '.csv')[0]
        test = sum(1 for _ in open(f'./staging/{csv_file}'))
        self.assertGreaterEqual(test, 2, 'Report should not be empty')

if __name__ == '__main__':
    unittest.main()