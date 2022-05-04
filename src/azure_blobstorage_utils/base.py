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
        self.create_local_dir(self.local_base_path)
        print("Using path: [{}] as local storage".format(self.local_base_path))

    @staticmethod
    def create_local_dir(dir_path: str):
        """

        Args:
            dir_path:

        Returns:

        """
        os.makedirs(dir_path, exist_ok=True)

    @staticmethod
    def get_folder_and_filename_from_full_path(filename: str):
        """

        Args:
            filename:

        Returns:

        """
        folder_name = None
        filename_split = filename.split("/")
        if len(filename_split) > 1:
            folder_name = "/".join(filename_split[:-1]) + "/"
            filename = filename_split[-1]
        return folder_name, filename

    def get_container_client(self, container_name: str):
        """

        Args:
            container_name:

        Returns:

        """
        return self.blob_service_client.get_container_client(container_name)

    def create_container(self, container_name: str):
        """

        Args:
            container_name:

        Returns:

        """
        try:
            self.blob_service_client.create_container(name=container_name)
        except ResourceExistsError:
            print("Container [{}] already exists. Skipping creation".format(container_name))
            pass

    def delete_container(self, container_name: str):
        """

        Args:
            container_name:

        Returns:

        """
        self.blob_service_client.delete_container(container_name)

    def get_blob_client(self, container_name: str, blob_name: str):
        """

        Args:
            container_name:
            blob_name:

        Returns:

        """
        return self.blob_service_client.get_blob_client(container=container_name,
                                                        blob=blob_name)

    def clean_local_folder(self):
        """

        Returns:

        """
        shutil.rmtree(self.local_base_path)

    def get_file_as_bytes(self, container_name: str, remote_file_name: str) -> bytes:
        """

        Args:
            container_name:
            remote_file_name:

        Returns:

        """
        blob_client = self.get_blob_client(container_name, remote_file_name)
        return blob_client.download_blob().readall()

    def get_file_as_text(self, container_name: str, remote_file_name: str) -> str:
        """

        Args:
            container_name:
            remote_file_name:

        Returns:

        """
        return self.get_file_as_bytes(container_name, remote_file_name).decode("UTF-8")

    def get_file_as_dict(self, container_name: str, remote_file_name: str) -> str:
        """

        Args:
            container_name:
            remote_file_name:

        Returns:

        """
        return json.loads(self.get_file_as_text(container_name, remote_file_name))

    def get_list_blobs_name(self, container_name: str, prefix: Optional[str] = None, return_list: bool = True) \
            -> Union[List[str], Iterable[str]]:
        """

        Args:
            container_name:
            prefix:
            return_list:

        Returns:

        """
        container_client = self.get_container_client(container_name)
        if container_client.exists():
            if prefix is not None:
                res = (blob.name for blob in container_client.list_blobs(name_starts_with=prefix))
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

        Args:
            container_name:
            remote_file_name:
            local_file_name:

        Returns:

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

    def download_folder(self, container_name: str, remote_folder: str, local_folder: Optional[str] = None):
        """

        Args:
            container_name:
            remote_folder:
            local_folder:

        Returns:

        """
        blob_gen = self.get_list_blobs_name(container_name, prefix=remote_folder, return_list=False)

        for blob_name in blob_gen:
            if local_folder is not None:
                self.download_file(container_name, blob_name, (local_folder + blob_name).replace("//", "/"))
            else:
                self.download_file(container_name, blob_name)

    def upload_file(self, container_name: str, local_file_name: str, remote_file_name: Optional[str] = None,
                    overwrite: bool = False):
        """

        Args:
            container_name:
            local_file_name:
            remote_file_name:
            overwrite:

        Returns:

        """
        container_client = self.get_container_client(container_name)
        if not container_client.exists():
            self.create_container(container_name)
            container_client = self.get_container_client(container_name)

        if remote_file_name is None:
            foldername, filename = self.get_folder_and_filename_from_full_path(local_file_name)
            remote_file_name = foldername.replace(self.local_base_path, "") + filename

        blob_client = container_client.get_blob_client(remote_file_name)
        try:
            with open(local_file_name, "rb") as data:
                blob_client.upload_blob(data, overwrite=overwrite)
        except ResourceExistsError:
            print("File [{}] already exists. Use overwrite = True if needed".format(remote_file_name))
            pass

    def _get_filepaths_from_folder(self, directory: str):
        """

        Args:
            directory:

        Returns:

        """
        file_paths = []  # List which will store all of the full filepaths.

        # Walk the tree.
        for root, directories, files in os.walk(directory):
            for filename in files:
                # Join the two strings in order to form the full filepath.
                filepath = os.path.join(root, filename)
                file_paths.append(filepath)  # Add it to the list.

        return file_paths

    def upload_folder(self, container_name: str, local_folder_name: str, remote_folder_name: Optional[str] = None,
                      overwrite: bool = False):
        """

        Args:
            container_name:
            local_folder_name:
            remote_folder_name:
            overwrite:

        Returns:

        """
        filepaths = self._get_filepaths_from_folder(local_folder_name)
        for filepath in filepaths:
            if remote_folder_name is not None:
                self.upload_file(container_name, filepath, (remote_folder_name + filepath).replace("//", "/"),
                                 overwrite)
            else:
                self.upload_file(container_name, filepath, filepath, overwrite)

    def upload_bytes(self, bytes: bytes, container_name: str, remote_file_name: str, overwrite: bool = False):
        """

        Args:
            bytes:
            container_name:
            remote_file_name:
            overwrite:

        Returns:

        """
        container_client = self.get_container_client(container_name)
        if not container_client.exists():
            self.create_container(container_name)
            container_client = self.get_container_client(container_name)

        blob_client = container_client.get_blob_client(remote_file_name)
        blob_client.upload_blob(bytes, overwrite=overwrite)

    def delete_blobs(self, container_name: str, remote_file_names: List[str]):
        """
        
        Args:
            container_name:
            remote_file_names:

        Returns:

        """
        container_client = self.get_container_client(container_name)
        if not container_client.exists():
            pass
        else:
            container_client.delete_blobs(*remote_file_names)
