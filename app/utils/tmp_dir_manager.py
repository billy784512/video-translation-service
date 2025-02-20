import uuid
import os
from typing import List

class TmpDirManager:
    def __init__(self, sub_folder_name: str = None) -> None:
        if not sub_folder_name:
            sub_folder_name = uuid.uuid4()
        self.folder_name = f"/tmp/{sub_folder_name}"

        if not os.path.exists(self.folder_name):
            os.makedirs(self.folder_name)

    def __del__(self):
        self.clear_folder()
        os.rmdir(self.folder_name)

    def get_files(self) -> List[str]:
        return [os.path.join(self.folder_name, f) for f in sorted(os.listdir(self.folder_name))]

    def get_folder_name(self) -> str:
        return self.folder_name

    def clear_folder(self) -> None:
        if os.path.exists(self.folder_name):
            for file in os.listdir(self.folder_name):
                file_path = os.path.join(self.folder_name, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)