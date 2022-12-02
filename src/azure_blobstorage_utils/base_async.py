import json
import os
import shutil
from typing import Iterable, List, Optional, Tuple, Union

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob.aio import BlobServiceClient


class BlobStorageBaseAsync:
    def __init__(
        self, connection_string: str, local_base_path: Optional[str] = "azure_tmp/"
    ):
        """

        Args:
            connection_string: Connection string to Azure Blob Storage
            local_base_path: local folder where data will be downloaded if path is not specified
        """
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        self.local_base_path = local_base_path
        self.create_local_dir(self.local_base_path)
        print("Using path: [{}] as local storage".format(self.local_base_path))

    @classmethod
    def create_local_dir(cls, directory_name: str):
        """
        Creates a local directory

        Args:
            directory_name: directory path

        Returns:

        """
        os.makedirs(directory_name, exist_ok=True)

    @staticmethod
    def get_directory_and_filename_from_full_path(file_name: str) -> Tuple[str]:
        """
        Split file_name and get folder_name and "real" file_name
        Args:
            file_name:

        Returns: directory_name, file_name

        """
        directory_name = None
        filename_split = file_name.split("/")
        if len(filename_split) > 1:
            directory_name = "/".join(filename_split[:-1]) + "/"
            file_name = filename_split[-1]
        return directory_name, file_name

    async def get_container_client(self, container_name: str):
        """
        Get container client from container_name

        Args:
            container_name: Name of the container

        Returns:

        """
        container_client = self.blob_service_client.get_container_client(container_name)
        if await container_client.exists():
            return container_client
        else:
            raise ResourceNotFoundError(
                "Container [{}] does not exist.".format(container_name)
            )

    async def create_container(self, container_name: str):
        """
        Create a container named container_name

        Args:
            container_name: Name of the container

        Returns:

        """
        try:
            await self.blob_service_client.create_container(name=container_name)
        except ResourceExistsError(
            "Container [{}] already exists. Skipping creation".format(container_name)
        ):
            pass

    async def delete_container(self, container_name: str):
        """
        Delete container named container_name

        Args:
            container_name: Name of the container

        Returns:

        """
        await self.blob_service_client.delete_container(container_name)

    def get_blob_client(self, container_name: str, blob_name: str):
        """
        Get blob client from container_name & blob_name

        Args:
            container_name: Name of the container
            blob_name: Name of the blob

        Returns:

        """
        return self.blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )

    def clean_local_folder(self):
        """
        Empty & delete local_folder (ie. local_base_path)

        Returns:

        """
        shutil.rmtree(self.local_base_path)

    async def get_file_as_bytes(
        self, container_name: str, remote_file_name: str
    ) -> bytes:
        """
        Get blob as bytes (in memory object)

        Args:
            container_name: Name of the container
            remote_file_name: Name of the blob

        Returns:

        """
        blob_client = self.get_blob_client(container_name, remote_file_name)
        stream = await blob_client.download_blob()
        data = await stream.readall()
        return data

    async def get_file_as_text(self, container_name: str, remote_file_name: str) -> str:
        """
        Get blob as text

        Args:
            container_name: Name of the container
            remote_file_name: Name of the blob

        Returns:

        """
        return await self.get_file_as_bytes(container_name, remote_file_name)

    async def get_file_as_dict(self, container_name: str, remote_file_name: str) -> str:
        """
        Get blob as dict (eg. for JSON files)

        Args:
            container_name: Name of the container
            remote_file_name: Name of the blob

        Returns:

        """
        raw_test = await self.get_file_as_text(container_name, remote_file_name)
        return json.loads(raw_test.decode("UTF-8"))

    async def get_list_blobs_name(
        self,
        container_name: str,
        prefix: Optional[str] = None,
        return_list: Optional[bool] = True,
    ) -> Union[List[str], Iterable[str]]:
        """
        Get list/generator of objects in container_name

        Args:
            container_name: Name of the container
            prefix: filter blobs with prefix
            return_list: if False, returns a generator

        Returns:

        """
        container_client = await self.get_container_client(container_name)
        if prefix is not None:
            res = []
            async for blob in container_client.list_blobs(name_starts_with=prefix):
                res.append(blob.name)
        else:
            res = []
            async for blob in container_client.list_blobs():
                res.append(blob.name)

        if not return_list:
            res = (y for y in res)
        return res

    async def download_file(
        self,
        container_name: str,
        remote_file_name: str,
        local_file_name: Optional[str] = None,
    ):
        """
        Download a blob named remote_file_name from a container named container_name

        Args:
            container_name: Name of the container
            remote_file_name: Name of the blob
            local_file_name: Name of the local file where the blob will be downloaded

        Returns:

        """
        if local_file_name is None:
            directory_name, file_name = self.get_directory_and_filename_from_full_path(
                remote_file_name
            )
            if directory_name is None:
                self.create_local_dir(self.local_base_path)
                local_file_name = self.local_base_path + file_name
            else:
                self.create_local_dir(self.local_base_path + directory_name)
                local_file_name = self.local_base_path + directory_name + file_name
        else:
            directory_name, file_name = self.get_directory_and_filename_from_full_path(
                local_file_name
            )
            if directory_name is None:
                self.create_local_dir(self.local_base_path)
                local_file_name = self.local_base_path + file_name
            else:
                self.create_local_dir(directory_name)
                local_file_name = directory_name + file_name

        blob_client = self.get_blob_client(container_name, remote_file_name)
        print("Downloading {} to {}".format(remote_file_name, local_file_name))
        with open(local_file_name, "wb") as my_blob:
            stream = await blob_client.download_blob()
            data = await stream.readall()
            my_blob.write(data)

    async def download_directory(
        self,
        container_name: str,
        remote_directory: str,
        local_directory: Optional[str] = None,
    ):
        """
        Download all blobs in directory

        Args:
            container_name: Name of the container
            remote_directory: Name of the remote directory
            local_directory: Name of the local directory where files will be downloaded

        Returns:

        """
        blob_gen = await self.get_list_blobs_name(
            container_name, prefix=remote_directory, return_list=False
        )

        for blob_name in blob_gen:
            if local_directory is not None:
                await self.download_file(
                    container_name,
                    blob_name,
                    (local_directory + blob_name).replace("//", "/"),
                )
            else:
                await self.download_file(container_name, blob_name)

    async def upload_file(
        self,
        container_name: str,
        local_file_name: str,
        remote_file_name: Optional[str] = None,
        overwrite: Optional[bool] = False,
    ):
        """
        Upload a local file to a blob

        Args:
            container_name: Name of the container
            local_file_name: Name of the local file
            remote_file_name: Name of the blob where file will be uploaded
            overwrite: set to True if needed

        Returns:

        """
        container_client = await self.get_container_client(container_name)
        if not await container_client.exists():
            await self.create_container(container_name)
            container_client = await self.get_container_client(container_name)

        if remote_file_name is None:
            directory_name, file_name = self.get_directory_and_filename_from_full_path(
                local_file_name
            )
            remote_file_name = (
                directory_name.replace(self.local_base_path, "") + file_name
            )

        blob_client = container_client.get_blob_client(remote_file_name)
        try:
            with open(local_file_name, "rb") as data:
                await blob_client.upload_blob(data, overwrite=overwrite)
        except ResourceExistsError as e:
            print(
                "File [{}] already exists. Use overwrite = True if needed".format(
                    remote_file_name
                )
            )
            raise e

    @classmethod
    def _get_file_paths_from_directory(cls, directory_name: str):
        """
        Get all file paths from local directory
        Args:
            directory_name: Name of the local directory

        Returns:

        """
        file_paths = []  # List which will store all of the full file paths.

        # Walk the tree.
        for root, _, files in os.walk(directory_name):
            for file_name in files:
                # Join the two strings in order to form the full filepath.
                file_path = os.path.join(root, file_name)
                file_paths.append(file_path)  # Add it to the list.

        return file_paths

    async def upload_directory(
        self,
        container_name: str,
        local_directory_name: str,
        remote_directory_name: Optional[str] = None,
        overwrite: Optional[bool] = False,
    ):
        """
        Upload local folder to blobs

        Args:
            container_name: Name of the container
            local_directory_name: Name of the local directory
            remote_directory_name: Name of the remote directory where file will be uploaded
            overwrite: set to True if needed

        Returns:

        """
        file_paths = self._get_file_paths_from_directory(local_directory_name)
        for filepath in file_paths:
            if remote_directory_name is not None:
                await self.upload_file(
                    container_name,
                    filepath,
                    (remote_directory_name + filepath).replace("//", "/"),
                    overwrite,
                )
            else:
                await self.upload_file(container_name, filepath, filepath, overwrite)

    async def upload_bytes(
        self,
        my_bytes: bytes,
        container_name: str,
        remote_file_name: str,
        overwrite: Optional[bool] = False,
    ):
        """
        Uploaded in memory byte object to blob

        Args:
            my_bytes: in memory object
            container_name: Name of the container
            remote_file_name: Name of the blob where object will be uploaded
            overwrite: set to True if needed

        Returns:

        """
        container_client = await self.get_container_client(container_name)
        if not await container_client.exists():
            await self.create_container(container_name)
            container_client = await self.get_container_client(container_name)

        blob_client = container_client.get_blob_client(remote_file_name)
        await blob_client.upload_blob(my_bytes, overwrite=overwrite)

    async def delete_blobs(self, container_name: str, remote_file_names: List[str]):
        """
        Delete list of blobs from container_name

        Args:
            container_name: Name of the container
            remote_file_names: list of blob names

        Returns:

        """
        container_client = await self.get_container_client(container_name)
        await container_client.delete_blobs(*remote_file_names)
