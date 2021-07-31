import hashlib
import os
import pathlib
import shlex
import tarfile
import tempfile
import traceback
import zipfile
from typing import Optional, Tuple

import parse
import py7zr
import rarfile
from fastapi import FastAPI, File, UploadFile, Response, status
import shutil

from warcio import ArchiveIterator

from logger import getLogger

l = getLogger("api")

app = FastAPI()


@app.post("/upload")
async def create_upload_file(response: Response, file: UploadFile = File(...)):
    l.debug(f"received file '{file.filename}'")
    with tempfile.TemporaryDirectory(prefix="recursive_archive_indexer_") as base_path:
        new_filepath = pathlib.Path(base_path + "/file" + pathlib.Path(file.filename).suffix)
        with open(new_filepath, "wb") as dest:
            l.debug(f"copying file '{file.filename}' into '{new_filepath}'")
            shutil.copyfileobj(file.file, dest)

        exec_file_output = exec_file_util(str(new_filepath))

        try:
            l.debug(f"indexing file '{new_filepath}'")
            data = index_archive(new_filepath, 4)
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
        "files": data["files"] if data is not None else [],
        "exec_file_output": exec_file_output,
    }


def hash_file(filename) -> Tuple[str, str]:
    BUF_SIZE = 2 ** 24  # 16MiB

    sha256 = hashlib.sha256()
    md5 = hashlib.md5()

    with open(filename, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)
            md5.update(data)

    return sha256.hexdigest(), md5.hexdigest()


def exec_file_util(real_filepath: str) -> str:
    l.debug(f"attempting to file '{real_filepath}'")
    try:
        return os.popen(f'file {shlex.quote(real_filepath)}').read()[len(real_filepath) + 2:-1]
    except Exception as e:
        l.exception(e)
        return "N/A"


def recurse(filename, tmp_dir, max_recursion, current_recursion, result, filename_prefix_recursive) -> None:
    l.debug(f"attempting to recurse into '{filename_prefix_recursive}'")
    deep_result = index_archive(pathlib.Path(tmp_dir + "/" + filename), max_recursion,
                                current_recursion + 1, filename_prefix_recursive)
    if deep_result is not None:
        result["files"].extend(deep_result["files"])


def new_entry(name: str, size_compressed: int, size_uncompressed: int, real_path: str) -> dict:
    file_util_output = exec_file_util(real_path)
    sha256, md5 = hash_file(real_path)

    return {
        "name": name,
        "size_compressed": size_compressed,
        "size_uncompressed": size_uncompressed,
        "file_util_output": file_util_output,
        "sha256": sha256,
        "md5": md5,
    }


def index_archive(filepath: pathlib.Path, max_recursion: int, current_recursion: int = 0,
                  filename_prefix_recursive: str = "") -> Optional[dict]:
    if current_recursion == max_recursion:
        l.debug(f"max recursion reached in '{filename_prefix_recursive}'")
        return None

    result = {
        "files": [],
    }

    if py7zr.is_7zfile(filepath):
        archive = py7zr.SevenZipFile(filepath, mode='r')
        file_infos = archive.list()

        for file_info in file_infos:
            try:
                if file_info.is_directory:
                    l.debug(f"skipping directory entry '{file_info.filename}'")
                    continue
                with tempfile.TemporaryDirectory(prefix="recursive_archive_indexer_") as tmp_dir:
                    full_filename = f"{filename_prefix_recursive}/{file_info.filename}"

                    l.debug(f"extracting '{file_info.filename}' into '{tmp_dir}'")
                    archive.extract(path=tmp_dir, targets=[file_info.filename])
                    archive.reset()

                    entry = new_entry(full_filename,
                                      file_info.compressed if file_info.compressed is not None else -1,
                                      file_info.uncompressed,
                                      f"{tmp_dir}/{file_info.filename}")
                    result["files"].append(entry)

                    recurse(file_info.filename, tmp_dir, max_recursion, current_recursion, result,
                            full_filename)
            except Exception as e:
                l.exception(e)
                continue

    elif zipfile.is_zipfile(filepath) or rarfile.is_rarfile(filepath) or rarfile.is_rarfile_sfx(filepath):
        if zipfile.is_zipfile(filepath):
            archive = zipfile.ZipFile(filepath, mode='r')
        else:
            archive = rarfile.RarFile(filepath, mode='r')

        file_infos = archive.infolist()

        for file_info in file_infos:
            try:
                if file_info.is_dir():
                    l.debug(f"skipping directory entry '{file_info.filename}'")
                    continue
                with tempfile.TemporaryDirectory(prefix="recursive_archive_indexer_") as tmp_dir:
                    full_filename = f"{filename_prefix_recursive}/{file_info.filename}"

                    l.debug(f"extracting '{full_filename}' into '{tmp_dir}'")
                    archive.extract(member=file_info.filename, path=tmp_dir)

                    entry = new_entry(full_filename,
                                      file_info.compress_size,
                                      file_info.file_size,
                                      f"{tmp_dir}/{file_info.filename}")

                    result["files"].append(entry)

                    recurse(file_info.filename, tmp_dir, max_recursion, current_recursion, result,
                            full_filename)
            except Exception as e:
                l.exception(e)
                continue

    elif tarfile.is_tarfile(filepath):
        archive = tarfile.TarFile(filepath, mode='r')

        file_infos = archive.getmembers()

        for file_info in file_infos:
            try:
                if file_info.isdir():
                    l.debug(f"skipping directory entry '{file_info.name}'")
                    continue
                with tempfile.TemporaryDirectory(prefix="recursive_archive_indexer_") as tmp_dir:
                    full_filename = f"{filename_prefix_recursive}/{file_info.name}"

                    l.debug(f"extracting '{full_filename}' into '{tmp_dir}'")
                    archive.extract(member=file_info.name, path=tmp_dir)

                    entry = new_entry(full_filename, -1, file_info.size,
                                      f"{tmp_dir}/{file_info.name}")

                    result["files"].append(entry)

                    recurse(file_info.name, tmp_dir, max_recursion, current_recursion, result,
                            full_filename)
            except Exception as e:
                l.exception(e)
                continue

    elif str(filepath).endswith(".warc") or \
            str(filepath).endswith(".arc") or \
            str(filepath).endswith(".warc.gz") or \
            str(filepath).endswith(".arc.gz"):
        with open(filepath, 'rb') as stream:
            try:
                for record in ArchiveIterator(stream):
                    try:
                        if record.rec_type == 'response':
                            with tempfile.TemporaryDirectory(prefix="recursive_archive_indexer_") as tmp_dir:
                                uri = record.rec_headers.get_header('WARC-Target-URI')
                                content_disposition = record.rec_headers.get_header('Content-Disposition')
                                content_disposition_filename = ""
                                if content_disposition:
                                    cdf = parse.search('filename="{}";', content_disposition)
                                    if cdf:
                                        content_disposition_filename = cdf[0]

                                real_path = f"{tmp_dir}/file"

                                fake_filename = '/'.join(
                                    uri.split('//')[1].split('/')[0:-1])

                                if len(content_disposition_filename) > 0:
                                    fake_filename += "cd-" + content_disposition_filename

                                full_filename = f"{filename_prefix_recursive}/{fake_filename}"

                                with open(real_path, 'wb') as f:
                                    f.write(record.content_stream().read())
                                    size = f.tell()

                                entry = new_entry(full_filename, size, size, real_path)
                                result["files"].append(entry)

                                recurse("file", tmp_dir, max_recursion, current_recursion, result, full_filename)
                    except Exception as e:
                        l.exception(e)
                        continue

            except Exception as e:
                l.exception(e)
    else:
        return None

    return result
