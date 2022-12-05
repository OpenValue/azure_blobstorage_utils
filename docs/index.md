# Welcome to Azure Blob Storage Utils

Azure Blob Storage utils provides a toolbox to interact with Azure Blob Storage.

# Usage

```py title="Define credentials"
connection_string = "DefaultEndpointsProtocol=https;AccountName=<storage_account_name>;AccountKey=<storage_account_key>;EndpointSuffix=core.windows.net"
container_name = "my_container"
```


```py title="Basic usage"
from azure_blobstorage_utils import BlobStorageBase

base_blob_helper = BlobStorageBase(connection_string)

# List files
base_blob_helper.get_list_blobs_name(container_name, prefix="xxxx")

# Download files
base_blob_helper.download_file(container_name, file_name)

# Upload files
base_blob_helper.upload_file(container_name, local_file_name)

# Upload bytes
base_blob_helper.upload_bytes(my_bytes_object, container_name, remote_file_name)
```

```py title="Extended usage"
from azure_blobstorage_utils import BlobStorageExtended  # need installation with extras !

# BlobStorageExtended inherits from BlobStorageBase
extended_blob_helper = BlobStorageExtended(connection_string)

# Get files as pandas dataframe
extended_blob_helper.get_file_as_pandas_df(container_name, file_name)

# Get files as numpy array
extended_blob_helper.get_image_as_numpy_array(container_name, file_name)

# Upload files
extended_blob_helper.upload_image_bytes_as_jpg_file(img_bytes,
                                                    container_name,
                                                    remote_file_name)
```
# Async

Basic & extended are available in async mode :

```py title="Async Mode"
from azure_blobstorage_utils import BlobStorageBaseAsync, BlobStorageExtendedAsync
```