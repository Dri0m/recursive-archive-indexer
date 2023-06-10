# recursive-archive-indexer

A tiny microservice thingy that, slightly inefficiently, returns file list and metadata of certain file archives. That's it.

It can read `.7z, .zip, .rar, .tar, .tar.gz, .tar.bz2, .tar.xz, .tar.zst .tar.zstd, .tgz, .warc, .arc, .warc.gz, .arc.gz`, if available, it detects the filetype via magic nubmers and headers and stuff, otherwise it's using file extensions.

## how to use
- requires `file` command to be present in PATH, otherwise it will spit errors (but it will work)
- the service is using fastapi for the server stuff, my preferred way of running these is `python -m uvicorn main:app --host $IP --port $PORT --workers $WORKER_COUNT`
- or call the indexing method directly and handle the output yourself
- the `index_archive` returns a flattened list of recursively listed file entries and the count of exceptions that occured during the process


### Run with Docker
First build the image using the Dockerfile
- `docker build -t recursive-archive-indexer .`

Now you can run the image. Use the environment variable `WEB_CONCURRENCY` to set the number of workers.
- `docker run -d --name recursive-archive-indexer -p 8372:8000 -e WEB_CONCURRENCY=4 recursive-archive-indexer`

## known issues
- `py7zr` doesn't seem to accept `tempfile.SpooledTemporaryFile` as a valid file object, so you can't really use the multipart upload endpoint right now (without copying the file, which is crappy)
- no test suite
- i improve it as i use it, so there most like are some bugs and issues
- i am 100% sure i'm not closing some files correctly
