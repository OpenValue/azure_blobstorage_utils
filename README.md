Azure Blob Storage Utils
========================
Purpose
-------
Python toolbox for Azure Blob Storage.

Installation
------------

```bash
pip install 
```

Build
-----

```bash
git clone 
cd azure-blobstorage-utils
python setup.py bdist_wheel
```

Basic usage
-----------

```python
from azure_blobstorage_utils import BlobStorageBase

base_blob_helper = BlobStorageBase(connection_string)

# List files
base_blob_helper.get_list_blobs_name(container_name, prefix="xxxx")

# Download files
base_blob_helper.download_file(container_name, file_name)

# Upload files
base_blob_helper.upload_file(container_name, local_file_name)

# Upload bytes
base_blob_helper.upload_bytes(bytes, container_name, remote_file_name)
```

Extended usage
-----------

```python
from azure_blobstorage_utils import BlobStorageExtended

# BlobStorageExtended inherits from BlobStorageBase
extended_blob_helper = BlobStorageExtended(connection_string)

# Get files as pandas dataframe
extended_blob_helper_blob_helper.get_file_as_pandas_df(container_name, file_name)

# Download files
base_blob_helper.download_file(container_name, file_name)

# Upload files
base_blob_helper.upload_file(container_name, local_file_name)

# Upload bytes
base_blob_helper.upload_bytes(bytes, container_name, remote_file_name)
```

Development
-----------
Install Anaconda:

See https://www.anaconda.com/distribution/#download-section

Edit `environment.yml` file and specify needed libraries

Install Anaconda local environment as below:

```bash
./install-conda-environment.sh
```

Activate Anaconda local environment as below:

```bash
conda activate ${PWD}/.conda
``