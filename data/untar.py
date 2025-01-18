import os
import tarfile

def extract_tar_files(tar_file_list, destination_folder):
    # Create the destination folder if it doesn't exist
    os.makedirs(destination_folder, exist_ok=True)

    for tar_file in tar_file_list:
        try:
            with tarfile.open(tar_file, 'r:*') as tar:
                tar.extractall(path=destination_folder)
            print(f"Successfully extracted {tar_file} to {destination_folder}")
        except Exception as e:
            print(f"Error extracting {tar_file}: {str(e)}")

# Example usage
tar_files = [
    r"E:\Downloads\2018_ProTennis.tar",
    r"E:\Downloads\2019_ProTennis.tar",
    r"E:\Downloads\2020_ProTennis.tar",
    r"E:\Downloads\2024_09_ProTennis.tar"
]
destination = '/path/to/extraction/folder'

extract_tar_files(tar_files, destination)