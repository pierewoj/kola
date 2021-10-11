# TODOs

* Remove file IO from tests (most likely change of the logstorage API is required)
    * https://endler.dev/2018/go-io-testing/
* Change logstorage API not to expose the reader
    * idea is to add ReadResult (containing response, is_more, and reader) that can be
      passed to the next read call and then the reader can be reused