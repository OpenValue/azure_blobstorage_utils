from .base import BlobStorageBase
import io
import sys

try:
    import pandas as pd
    import cv2
    import numpy as np
except ModuleNotFoundError as moduleErr:
    print("[Error]: Failed to import (Module Not Found) {}.".format(moduleErr.args[0]))
    print("Please install with extras")
    sys.exit(1)
except ImportError as impErr:
    print("[Error]: Failed to import (Import Error) {}.".format(impErr.args[0]))
    print("Please install with extras")
    sys.exit(1)


class BlobStorageExtended(BlobStorageBase):
    def __init__(self, connection_string: str, local_base_path: str = "azure_tmp/"):
        super().__init__(connection_string, local_base_path)

    def get_file_as_pandas_df(self, container_name: str, file_path: str, **kwargs) -> pd.DataFrame:
        """

        :param container_name:
        :param file_path:
        :param kwargs:
        :return:
        """
        stream = self.get_file_as_bytes(container_name, file_path)
        if file_path.endswith(".csv") | file_path.endswith(".txt"):
            return pd.read_csv(io.BytesIO(stream), **kwargs)
        elif file_path.endswith(".parquet"):
            return pd.read_parquet(io.BytesIO(stream), **kwargs)
        elif file_path.endswith(".json"):
            return pd.read_json(io.BytesIO(stream), **kwargs)
        elif file_path.endswith(".xls") | file_path.endswith(".xlsx"):
            return pd.read_excel(io.BytesIO(stream), **kwargs)
        else:
            print("Extension not recognized - only ['csv','txt','parquet','json','xls','xlsx'] are supported.")
            return None

    def get_image_as_numpy_array(self, container_name: str, file_path: str) -> np.ndarray:
        """

        :param container_name:
        :param file_path:
        :return:
        """
        stream = self.get_file_as_bytes(container_name, file_path)
        img = cv2.imdecode(np.frombuffer(stream, np.uint8), cv2.IMREAD_COLOR)
        return img

    def upload_image_bytes_as_jpg_file(self, img: bytes, container_name: str, remote_file_name: str,
                                       overwrite: bool = False):
        """

        :param container_name:
        :param img:
        :param remote_file_name:
        :param overwrite:
        :return:
        """
        _, img_encode = cv2.imencode('.jpg', img)
        img_bytes = img_encode.tobytes()
        self.upload_bytes(img_bytes, container_name, remote_file_name, overwrite)
