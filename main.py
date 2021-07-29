import pathlib
import tempfile
import traceback
from typing import Optional

import py7zr
from fastapi import FastAPI, File, UploadFile, Response, status
import shutil

from logger import getLogger

l = getLogger("api")

app = FastAPI()


@app.post("/upload")
async def create_upload_file(response: Response, file: UploadFile = File(...)):
    l.debug(f"received file '{file.filename}'")
    with tempfile.TemporaryDirectory(prefix="recursive_archive_indexer_") as base_path:
        new_filepath = pathlib.Path(base_path + "/file" + pathlib.Path(file.filename).suffix)
        with open(new_filepath, "wb") as dest:
            l.debug(f"copying file '{file.filename}' into '{new_filepath}'.")
            shutil.copyfileobj(file.file, dest)

        try:
            data = index_archive(new_filepath, 2)
        except Exception as e:
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            return {
                "exception": "".join(
                    traceback.format_exception(
                        etype=type(e), value=e, tb=e.__traceback__
                    )
                )
            }

    return {
        "archive_filename": file.filename,
        "archive_size": file,
        "archive_size_uncompressed": None,
    }


def index_archive(filepath: pathlib.Path, max_recursion: int, current_recursion: int = 0) -> Optional[dict]:
    if current_recursion == max_recursion:
        return None

    result = {
        "files": []
    }

    if str(filepath).endswith(".7z"):
        archive = py7zr.SevenZipFile(filepath, mode='r')
        uncompressed_size = archive.archiveinfo().uncompressed
        file_infos = archive.list()

        l.debug(f"uncompressed_size: {uncompressed_size}")

        for file_info in file_infos:
            if file_info.is_directory:
                l.debug(f"skipping directory entry '{file_info.filename}'")
                continue
            with tempfile.TemporaryDirectory(prefix="recursive_archive_indexer_") as tmp_dir:
                l.debug(f"extracting '{file_info.filename}' into '{tmp_dir}'")
                archive.extract(tmp_dir, [file_info.filename])
                archive.reset()

                entry = {"name": file_info.filename, "size_compressed": file_info.compressed,
                         "size_uncompressed": file_info.uncompressed}

                l.debug(f"attempting to recurse into '{file_info.filename}'")
                entry["files"] = index_archive(pathlib.Path(file_info.filename), max_recursion, current_recursion + 1)

                result["files"].append(entry)
    else:
        return None

    l.debug(result)
    return result


index_archive(pathlib.Path("insider.7z"), 2)
index_archive(pathlib.Path("outsider.7z"), 2)
