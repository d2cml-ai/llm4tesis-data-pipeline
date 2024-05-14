import os
import shutil
from Constants import TEMP_DIR_PATH

def cleanup() -> None:
        files: list[str] = [filename for filename in os.listdir(TEMP_DIR_PATH) if filename != ".keep"]
        for filename in files:
                file_path: str = os.path.join(TEMP_DIR_PATH, filename)
                try:
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                                os.unlink(file_path)
                        elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                except Exception as e:
                        print('Failed to delete %s. Reason: %s' % (file_path, e))
        return