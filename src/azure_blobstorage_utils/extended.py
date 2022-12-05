import io
import os
import sys
from typing import Dict, Optional

from .base import BlobStorageBase

try:
    import numpy as np
    import pandas as pd
    from simplejpeg import decode_jpeg, encode_jpeg
except ModuleNotFoundError as moduleErr:
    print("[Error]: Failed to import (Module Not Found) {}.".format(moduleErr.args[0]))
    print("Please install with extras")
    sys.exit(1)
except ImportError as impErr:
    print("[Error]: Failed to import (Import Error) {}.".format(impErr.args[0]))
    print("Please install with extras")
    sys.exit(1)


class BlobStorageExtended(BlobStorageBase):
    def __init__(
        self, connection_string: str, local_base_path: Optional[str] = "azure_tmp/"
    ):
        """

        Args:
            connection_string: Connection string to Azure Blob Storage
            local_base_path: local folder where data will be downloaded if path is not specified
        """
        super().__init__(connection_string, local_base_path)

    def get_file_as_pandas_df(
        self, container_name: str, remote_file_name: str, **kwargs: Optional[Dict]
    ) -> pd.DataFrame:
        """
        Get a blob & load it as a pandas DataFrame

        Args:
            container_name: Name of the container
            remote_file_name: Name of the blob
            **kwargs: add any kwarg that you would put in pd.read_* methods.

        Returns: a pandas DataFrame

        """
        stream = self.get_file_as_bytes(container_name, remote_file_name)
        if remote_file_name.endswith(".csv") | remote_file_name.endswith(".txt"):
            return pd.read_csv(io.BytesIO(stream), **kwargs)
        elif remote_file_name.endswith(".parquet"):
            return pd.read_parquet(io.BytesIO(stream), **kwargs)
        elif remote_file_name.endswith(".json"):
            return pd.read_json(io.BytesIO(stream), **kwargs)
        elif remote_file_name.endswith(".xls") | remote_file_name.endswith(".xlsx"):
            return pd.read_excel(io.BytesIO(stream), **kwargs)
        else:
            raise ValueError(
                "Extension not recognized - only ['csv','txt','parquet','json','xls','xlsx'] are supported."
            )

    def get_image_as_numpy_array(
        self, container_name: str, remote_file_name: str
    ) -> np.ndarray:
        """
        Get an image file from blob & load it as numpy array

        Args:
            container_name: Name of the container
            remote_file_name: Name of the blob

        Returns: a RGB numpy array of the image

        """
        stream = self.get_file_as_bytes(container_name, remote_file_name)
        return decode_jpeg(stream)

    def upload_image_bytes_as_jpg_file(
        self,
        img: np.ndarray,
        container_name: str,
        remote_file_name: str,
        overwrite: Optional[bool] = False,
        quality: Optional[int] = 95,
    ):
        """
        Upload an in memory image numpy array as a jpg file

        Args:
            img: a RGB numpy array
            container_name: Name of the container
            remote_file_name: Name of the blob where image will be uploaded
            overwrite: set to True if needed

        Returns:

        """
        self.upload_bytes(
            encode_jpeg(img, quality=quality),
            container_name,
            remote_file_name,
            overwrite,
        )

    def upload_pandas_df(
        self,
        df: pd.DataFrame,
        container_name: str,
        remote_file_name: str,
        overwrite: Optional[bool] = False,
        **kwargs: Optional[Dict]
    ):
        """
        Upload a in memory pandas DataFrame to a blob
        Args:
            df: a pandas DataFrame
            container_name: Name of the container
            remote_file_name: Name of the blob where the dataframe will be uploaded
            overwrite: set to True if needed
            **kwargs: add any kwarg that you would put in pd.to_* methods.

        Returns:

        """
        foldername, filename = self.get_directory_and_filename_from_full_path(
            remote_file_name
        )
        if filename.endswith(".csv") | filename.endswith(".txt"):
            df.to_csv(self.local_base_path + filename, **kwargs)
        elif remote_file_name.endswith(".parquet"):
            df.to_parquet(self.local_base_path + filename, **kwargs)
        elif remote_file_name.endswith(".json"):
            df.to_json(self.local_base_path + filename, **kwargs)
        elif remote_file_name.endswith(".xls") | filename.endswith(".xlsx"):
            df.to_excel(self.local_base_path + filename, **kwargs)
        else:
            print(
                "Extension not recognized - only ['csv','txt','parquet','json','xls','xlsx'] are supported."
            )
        if os.path.exists(self.local_base_path + filename):
            self.upload_file(
                container_name,
                local_file_name=self.local_base_path + filename,
                remote_file_name=remote_file_name,
                overwrite=overwrite,
            )
