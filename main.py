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
from enum import Enum

from pyzstd import ZstdFile
from warcio import ArchiveIterator

from logger import getLogger

l = getLogger("api")

app = FastAPI()


@app.post("/upload")
async def create_upload_file(response: Response, file: UploadFile = File(...)):
    l.debug(f"received file '{file.filename}'")
    try:
        l.debug(f"indexing file '{file.filename}'")
        data = index_archive(pathlib.Path(file.filename), 4, file_object=file.file)
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
        "files": data["files"] if data is not None else [],
        "indexing_errors": data["indexing_errors"],
    }


# just hand over absolute path to the file instead of uploading it, saves some unnecessary copying ay?
@app.post("/provide-path")
async def create_upload_file(response: Response, path: str):
    try:
        with open(path, 'rb') as f:
            l.debug(f"indexing file '{path}'")
            data = index_archive(pathlib.Path(path), 4, file_object=f)
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
        "archive_filename": f.name,
        "files": data["files"] if data is not None else [],
        "indexing_errors": data["indexing_errors"],
    }


class ArchiveType(Enum):
    SEVEN_ZIP = 1
    ZIP = 2
    RAR = 3
    TAR = 4
    TAR_ZST = 5
    WEB_ARCHIVE = 6


def _is_filetype(func, filepath, file_object):
    if file_object is None:
        return func(filepath)

    b = func(file_object)
    file_object.seek(0)
    return b


def _open_filetype(func, filepath, file_object, *args, **kwargs):
    if file_object is None:
        return func(filepath)

    return func(file_object, *args, **kwargs)


class UnsupportedFileType(Exception):
    pass


class UniversalArchiveIterator:

    def __init__(self, filepath, file_object):
        self.filepath = filepath
        self.file_object = file_object
        self.archive_type = self._get_archive_type()
        self.error_counter = 0

    def _get_archive_type(self):
        if _is_filetype(py7zr.is_7zfile, self.filepath, self.file_object):
            return ArchiveType.SEVEN_ZIP
        elif _is_filetype(zipfile.is_zipfile, self.filepath, self.file_object):
            return ArchiveType.ZIP
        elif _is_filetype(rarfile.is_rarfile, self.filepath, self.file_object) or \
                _is_filetype(rarfile.is_rarfile_sfx, self.filepath, self.file_object):
            return ArchiveType.RAR
        elif str(self.filepath).endswith(".warc") or \
                str(self.filepath).endswith(".arc") or \
                str(self.filepath).endswith(".warc.gz") or \
                str(self.filepath).endswith(".arc.gz"):
            return ArchiveType.WEB_ARCHIVE
        elif _is_filetype(tarfile.is_tarfile, self.filepath, self.file_object) or \
                str(self.filepath).endswith(".tar.gz") or \
                str(self.filepath).endswith(".tar.bz2") or \
                str(self.filepath).endswith(".tar.xz"):
            return ArchiveType.TAR
        elif str(self.filepath).endswith(".tar.zst") or \
                str(self.filepath).endswith(".tar.zstd"):
            return ArchiveType.TAR_ZST
        else:
            raise UnsupportedFileType()

    def iterate(self):
        if self.archive_type == ArchiveType.SEVEN_ZIP:
            archive = _open_filetype(py7zr.SevenZipFile, self.filepath, self.file_object, mode='r')
            file_infos = archive.list()

            for file_info in file_infos:
                try:
                    if file_info.is_directory:
                        continue
                    with tempfile.TemporaryDirectory(prefix="recursive_archive_indexer_") as tmp_dir:
                        archive.extract(path=tmp_dir, targets=[file_info.filename])
                        archive.reset()

                        filename = file_info.filename
                        real_path = f"{tmp_dir}/{filename}"
                        yield real_path, filename

                except Exception as e:
                    l.exception(e)
                    self.error_counter += 1

        elif self.archive_type == ArchiveType.ZIP or self.archive_type == ArchiveType.RAR:
            if self.archive_type == ArchiveType.ZIP:
                archive = _open_filetype(zipfile.ZipFile, self.filepath, self.file_object, mode='r')
            else:
                archive = _open_filetype(rarfile.RarFile, self.filepath, self.file_object, mode='r')

            file_infos = archive.infolist()

            for file_info in file_infos:
                try:
                    if file_info.is_dir():
                        continue
                    with tempfile.TemporaryDirectory(prefix="recursive_archive_indexer_") as tmp_dir:
                        archive.extract(member=file_info.filename, path=tmp_dir)

                        filename = file_info.filename
                        real_path = f"{tmp_dir}/{filename}"
                        yield real_path, filename

                except Exception as e:
                    l.exception(e)
                    self.error_counter += 1

        elif self.archive_type == ArchiveType.WEB_ARCHIVE:
            if self.file_object is not None:
                self.file_object.seek(0)
                stream = self.file_object
            else:
                stream = open(self.filepath, 'rb')

            for record in ArchiveIterator(stream):
                try:
                    if record.rec_type != 'response':
                        continue

                    with tempfile.TemporaryDirectory(prefix="recursive_archive_indexer_") as tmp_dir:
                        uri = record.rec_headers.get_header('WARC-Target-URI')
                        content_disposition = record.rec_headers.get_header('Content-Disposition')
                        content_disposition_filename = ""
                        if content_disposition:
                            cdf = parse.search('filename="{}";', content_disposition)
                            if cdf:
                                content_disposition_filename = cdf[0]

                        filename = '/'.join(
                            uri.split('//')[1].split('/')[0:-1])

                        if len(content_disposition_filename) > 0:
                            filename += "cd-" + content_disposition_filename

                        real_path = f"{tmp_dir}/file"

                        with open(real_path, 'wb') as f:
                            f.write(record.content_stream().read())

                        yield real_path, filename

                except Exception as e:
                    l.exception(e)
                    self.error_counter += 1

        elif self.archive_type == ArchiveType.TAR or self.archive_type == ArchiveType.TAR_ZST:
            if self.archive_type == ArchiveType.TAR:
                if self.file_object is not None:
                    self.file_object.seek(0)
                    archive = tarfile.open(fileobj=self.file_object, mode='r')
                else:
                    archive = tarfile.open(self.filepath, mode='r')
            else:
                archive = _open_filetype(ZstdTarFile, self.filepath, self.file_object, mode='r')

            file_infos = archive.getmembers()

            for file_info in file_infos:
                try:
                    if file_info.isdir():
                        continue

                    with tempfile.TemporaryDirectory(prefix="recursive_archive_indexer_") as tmp_dir:
                        archive.extract(member=file_info.name, path=tmp_dir)

                        filename = file_info.name
                        real_path = f"{tmp_dir}/{filename}"
                        yield real_path, filename

                except Exception as e:
                    l.exception(e)
                    self.error_counter += 1
        else:
            raise UnsupportedFileType()


class ZstdTarFile(tarfile.TarFile):
    def __init__(self, name, mode='r', *, level_or_option=None, zstd_dict=None, **kwargs):
        l.debug("attempting to open a zstandard tar file")

        self.zstd_file = ZstdFile(name, mode,
                                  level_or_option=level_or_option,
                                  zstd_dict=zstd_dict)
        try:
            super().__init__(fileobj=self.zstd_file, mode=mode, **kwargs)
        except:
            self.zstd_file.close()
            raise

    def close(self):
        try:
            super().close()
        finally:
            self.zstd_file.close()


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


def new_entry(name: str, real_path: str) -> dict:
    file_util_output = exec_file_util(real_path)
    sha256, md5 = hash_file(real_path)

    with open(real_path, "rb") as f:
        size_uncompressed = f.tell()

    return {
        "name": name,
        "size_uncompressed": size_uncompressed,
        "file_util_output": file_util_output,
        "sha256": sha256,
        "md5": md5,
    }


def index_archive(filepath: pathlib.Path, max_recursion: int, current_recursion: int = 0,
                  filename_prefix_recursive: str = "", file_object=None) -> Optional[dict]:
    if current_recursion == max_recursion:
        l.debug(f"max recursion reached in '{filename_prefix_recursive}'")
        return None

    result = {
        "files": [],
        "indexing_errors": 0
    }

    error_counter = 0

    try:
        try:
            uai = UniversalArchiveIterator(filepath, file_object)
        except UnsupportedFileType:
            return None

        for real_path, filename in uai.iterate():
            full_filename = f"{filename_prefix_recursive}/{filename}"
            error_counter = uai.error_counter

            entry = new_entry(full_filename, real_path)
            result["files"].append(entry)

            l.debug(f"attempting to recurse into '{full_filename}'")
            deep_result = index_archive(pathlib.Path(real_path), max_recursion, current_recursion + 1, full_filename)
            if deep_result is not None:
                result["files"].extend(deep_result["files"])

    except Exception as e:
        l.exception(e)
        result["indexing_errors"] += error_counter

    result["indexing_errors"] += error_counter

    return result
