# recursive-archive-indexer

A tiny microservice thingy that, slightly inefficiently, returns file list and metadata of certain file archives. That's it.

## how to use
- requires `file` command to be present in PATH, otherwise it will spit errors (but it will work)
- the service is using fastapi for the server stuff, my preferred way of running these is `python -m uvicorn main:app --host $IP --port $PORT --workers $WORKER_COUNT`
- or call the indexing method directly and handle the output yourself
- the `index_archive` returns a flattened list of recursively listed file entries and the count of exceptions that occured during the process

## known issues
- `py7zr` doesn't seem to accept `tempfile.SpooledTemporaryFile` as a valid file object, so you can't really use the multipart upload endpoint right now (without copying the file, which is crappy)
- no test suite
- i improve it as i use it, so there most like are some bugs and issues
