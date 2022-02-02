from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient
from typing import List
import os
import shutil
from typing import Optional, Iterable, Union
import json


class BlobStorageBase:
    def __init__(self, connection_string: str, local_base_path: str = "azure_tmp/"):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.local_base_path = local_base_path
        print("Using path: [{}] as local storage".format(self.local_base_path))

    @staticmethod
    def create_local_dir(dir_path: str):
        """

        :param dir_path:
        :return:
        """
        os.makedirs(dir_path, exist_ok=True)

    @staticmethod
    def get_folder_and_filename_from_full_path(filename: str):
        """

        :param filename:
        :return:
        """
        foldername = None
        filename_split = filename.split("/")
        if len(filename_split) > 1:
            foldername = "/".join(filename_split[:-1]) + "/"
            filename = filename_split[-1]
        return foldername, filename

    def get_container_client(self, container_name: str):
        """

        :param container_name:
        :return:
        """
        return self.blob_service_client.get_container_client(container_name)

    def create_container(self, container_name: str):
        """

        :param container_name:
        :return:
        """
        try:
            self.blob_service_client.create_container(name=container_name)
        except ResourceExistsError:
            print("Container [{}] already exists. Skipping creation".format(container_name))
            pass

    def get_blob_client(self, container_name: str, blob_name: str):
        """

        :param container_name:
        :param blob_name:
        :return:
        """
        return self.blob_service_client.get_blob_client(container=container_name,
                                                        blob=blob_name)

    def clean_local_folder(self):
        """

        :return:
        """
        shutil.rmtree(self.local_base_path)

    def get_file_as_bytes(self, container_name: str, remote_file_name: str) -> bytes:
        """

        :param container_name:
        :param remote_file_name:
        :return:
        """
        blob_client = self.get_blob_client(container_name, remote_file_name)
        return blob_client.download_blob().readall()

    def get_file_as_text(self, container_name: str, remote_file_name: str) -> str:
        """

        :param container_name:
        :param remote_file_name:
        :return:
        """
        return self.get_file_as_bytes(container_name, remote_file_name).decode("UTF-8")

    def get_file_as_dict(self, container_name: str, remote_file_name: str) -> str:
        """

        :param container_name:
        :param remote_file_name:
        :return:
        """
        return json.loads(self.get_file_as_text(container_name, remote_file_name))

    def get_list_blobs_name(self, container_name: str, prefix: Optional[str] = None, return_list: bool = True) \
            -> Union[List[str], Iterable[str]]:
        """

        :param container_name:
        :param prefix:
        :param return_list:
        :return:
        """
        container_client = self.get_container_client(container_name)
        if container_client.exists():
            if prefix is not None:
                res = (blob.name for blob in container_client.list_blobs(name_starts_with=prefix))
                if return_list:
                    res = list(res)
            else:
                res = (blob.name for blob in container_client.list_blobs())
                if return_list:
                    res = list(res)
            return res

        else:
            print("Container [{}] doesn't exists".format(container_name))
        return None

    def download_file(self, container_name: str, remote_file_name: str, local_file_name: str = None):
        """

        :param container_name:
        :param remote_file_name:
        :param local_file_name:
        :return:
        """
        if local_file_name is None:
            foldername, filename = self.get_folder_and_filename_from_full_path(remote_file_name)
            if foldername is None:
                self.create_local_dir(self.local_base_path)
                local_file_name = self.local_base_path + filename
            else:
                self.create_local_dir(self.local_base_path + foldername)
                local_file_name = self.local_base_path + foldername + filename
        else:
            foldername, filename = self.get_folder_and_filename_from_full_path(local_file_name)
            if foldername is None:
                self.create_local_dir(self.local_base_path)
                local_file_name = self.local_base_path + filename
            else:
                self.create_local_dir(foldername)
                local_file_name = foldername + filename

        blob_client = self.get_blob_client(container_name, remote_file_name)
        print("Downloading {} to {}".format(remote_file_name, local_file_name))
        with open(local_file_name, "wb") as my_blob:
            my_blob.write(blob_client.download_blob().readall())

    def upload_file(self, container_name: str, local_file_name: str, remote_file_name: Optional[str] = None,
                    overwrite: bool = False):
        """

        :param container_name:
        :param local_file_name:
        :param remote_file_name:
        :param overwrite:
        :return:
        """
        container_client = self.get_container_client(container_name)
        if not container_client.exists():
            self.create_container(container_name)
            container_client = self.get_container_client(container_name)

        if remote_file_name is None:
            foldername, filename = self.get_folder_and_filename_from_full_path(local_file_name)
            remote_file_name = foldername.replace(self.local_base_path) + filename

        blob_client = container_client.get_blob_client(remote_file_name)
        try:
            with open(local_file_name, "rb") as data:
                blob_client.upload_blob(data, overwrite=overwrite)
        except ResourceExistsError:
            print("File [{}] already exists. Use overwrite = True if needed".format(remote_file_name))
            pass

    def upload_bytes(self, bytes: bytes, container_name: str, remote_file_name: str, overwrite: bool = False):
        """

        :param bytes:
        :param container_name:
        :param remote_file_name:
        :param overwrite:
        :return:
        """
        container_client = self.get_container_client(container_name)
        if not container_client.exists():
            self.create_container(container_name)
            container_client = self.get_container_client(container_name)

        blob_client = container_client.get_blob_client(remote_file_name)
        blob_client.upload_blob(bytes, overwrite=overwrite)
