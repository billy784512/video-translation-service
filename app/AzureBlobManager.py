import os
import logging
from typing import List
from concurrent.futures import ThreadPoolExecutor

from azure.storage.blob import BlobServiceClient, PublicAccess

class AzureBlobManager:
    def __init__(self, connection_string: str):
        self.set_connection_string(connection_string)

    def set_connection_string(self, connection_string: str) -> None:
        self.connection_string = connection_string
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    def download_blob_to_local(self, container_name: str, blob_name: str, num_threads: int = 4) -> str:
        def download_range(range_start, range_end, thread_idx):
            try:
                logging.info(f"Thread-{thread_idx} downloading range {range_start}-{range_end}")
                stream = blob_client.download_blob(offset=range_start, length=(range_end - range_start + 1))
                with open(local_file_path, "r+b") as file:
                    file.seek(range_start)
                    file.write(stream.readall())
                logging.info(f"Thread-{thread_idx} completed range {range_start}-{range_end}")
            except Exception as e:
                logging.error(f"Thread-{thread_idx} failed: {e}")
                raise

        try:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_properties = blob_client.get_blob_properties()
            blob_size = blob_properties.size
            logging.info(f"Blob size: {blob_size} bytes")

            chunk_size = blob_size // num_threads
            ranges = [(i * chunk_size, (i + 1) * chunk_size - 1) for i in range(num_threads)]
            ranges[-1] = (ranges[-1][0], blob_size - 1)

            local_file_path = f"/tmp/{blob_name}"
            with open(local_file_path, "wb") as file:
                file.truncate(blob_size)

            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                tasks = [executor.submit(download_range, start, end, idx) for idx, (start, end) in enumerate(ranges)]
                for task in tasks:
                    task.result()

            logging.info(f"Blob downloaded to {local_file_path}.")
            return local_file_path
        except Exception as e:
            logging.error(f"Failed to download blob: {e}")
            raise

    def download_directory_to_local(self, container_name: str, directory_name: str, file_type: str = None) -> str:
        try:
            local_directory = f"/tmp/{directory_name}"
            if not os.path.exists(local_directory):
                os.makedirs(local_directory)

            if not directory_name.endswith("/"):
                directory_name += "/"

            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = container_client.list_blobs(name_starts_with=directory_name)

            with ThreadPoolExecutor(max_workers=3) as executor:
                tasks = []
                for blob in blobs:
                    blob_name = blob.name

                    logging.info(f"find {blob_name} in {directory_name}...")

                    if file_type and not blob_name.endswith(file_type):
                        logging.info(f"skip {blob_name}...")
                        continue
                        
                    logging.info(f"download {blob_name}...")

                    local_file_path = os.path.join(local_directory, os.path.relpath(blob_name, directory_name))

                    # Ensure the local directory structure matches the blob directory structure
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                    # Add download task
                    tasks.append(executor.submit(self.download_blob_to_local, container_name, blob_name, 3))

                # Wait for all tasks to complete
                for task in tasks:
                    task.result()

            logging.info(f"All blobs in directory '{directory_name}' downloaded to '/tmp/{directory_name}'.")
            return local_directory
        except Exception as e:
            logging.error(f"Failed to download directory: {e}")
            raise

    def upload_file(self, container_name:str, blob_name: str, file_path: str) -> None:
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True, max_concurrency=4)
            logging.info(f"Uploaded {file_path} to {blob_name}.")
        except Exception as e:
            logging.error(f"Failed to upload {file_path}: {e}")
            raise

    def upload_chunks(self, container_name: str, origin_video_name: str, chunked_files: List[str]) -> None:
        self.__upload_chunks(container_name, origin_video_name, chunks = chunked_files)

    def upload_chunk(self, container_name: str, origin_video_name: str, chunk_name: str, chunk_path: str) -> None:
        self.__upload_chunks(container_name, origin_video_name, chunk = (chunk_name, chunk_path))

    def list_files_in_folder(self, container_name: str, folder_name: str, file_type: str = None) -> List[str]:
        container_client = self.blob_service_client.get_container_client(container_name)

        if not folder_name.endswith("/"):
            folder_name += "/"

        blobs = container_client.list_blob_names(name_starts_with=folder_name)
        
        if file_type:
            result = [blob for blob in blobs if blob.endswith(file_type)]
        else:
            result = [blob for blob in blobs]

        return result
    
    def count_files_in_folder(self, container_name: str, folder_name: str) -> int:
        container_client = self.blob_service_client.get_container_client(container_name)

        if not folder_name.endswith("/"):
            folder_name += "/"

        blobs = container_client.list_blob_names(name_starts_with=folder_name)
        file_count = sum(1 for _ in blobs)
        return file_count

    def get_blob_url(self, container_name: str, blob_name: str) -> str:
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        return blob_client.url


    def __upload_chunks(self, container_name: str, origin_video_name: str, chunks: List[str] = None, chunk: tuple = None) -> None:
        self.__ensure_container_exists(container_name)

        with ThreadPoolExecutor(max_workers=3) as executor:
            tasks = []
            if chunks:
                for chunk_path in chunks:
                    blob_name = f"{origin_video_name}/{os.path.basename(chunk_path)}"
                    tasks.append(executor.submit(self.upload_file, container_name, blob_name, chunk_path))
            elif chunk:
                blob_name = f"{origin_video_name}/{chunk[0]}"
                tasks.append(executor.submit(self.upload_file, container_name, blob_name, chunk[1]))
            for task in tasks:
                task.result()

    def __ensure_container_exists(self, container_name: str) -> None:
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            if not container_client.exists():
                logging.info(f"Container '{container_name}' does not exist. Creating it...")
                container_client.create_container(public_access=PublicAccess.CONTAINER)
                logging.info(f"Container '{container_name}' created successfully.")
            else:
                logging.info(f"Container '{container_name}' already exists.")
        except Exception as e:
            logging.error(f"Error ensuring container '{container_name}' exists: {e}")
            raise