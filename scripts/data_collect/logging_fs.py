#!/usr/bin/env python3
"""
LoggingFileSystem —— 湖仓 S3 自适应预取项目 (project_notes.md §7 统一采集口子 #2)

目的：
  在 pyarrow 读取 Parquet 时，**拦截每一次真实的 range-GET**
  (object_key, offset, length)，落成 trace CSV，喂给
  `lakehouse_access_heatmap.py` 和后续的 contextual bandit。

为什么在 pyarrow 层拦截而不是 DuckDB：
  - pyarrow 走 Python，能在 filesystem 层完整截获 read(offset,length)，
    且自带列裁剪 + row group 裁剪 → 拿到的就是 footer→column index→
    选中 row group 的列分片这种真实格式感知访问 pattern。
  - DuckDB 读 Parquet 在 C++ 里，Python 层拦不到偏移。

关键认知：Parquet 的 offset 由文件格式内在决定，本地磁盘读出来的访问
  pattern 与字节躺在 S3 上完全一致，所以采集 trace **不需要连 S3**。

用法见 run_tpch_trace.py。核心 API：
  logger = ReadLogger()
  fs = make_logging_fs(logger, root="data/tpch/sf1")
  ds.dataset(path, format="parquet", filesystem=fs).to_table(columns=..., filter=...)
  logger.to_csv("trace.csv")
"""
from __future__ import annotations

import os
import threading
import time

import pyarrow as pa
import pyarrow.fs as pafs


class ReadLogger:
    """线程安全地收集 (timestamp, object_key, offset, length, file_size, query_id)。"""

    def __init__(self) -> None:
        self.records: list[tuple] = []
        self._lock = threading.Lock()
        self._t0 = time.time()
        # 由调用方在每个查询前设置，标注该批 read 属于哪个查询/负载
        self.current_query: str = "?"

    def record(self, object_key: str, offset: int, length: int, file_size: int) -> None:
        ts = time.time() - self._t0
        with self._lock:
            self.records.append(
                (ts, object_key, int(offset), int(length), int(file_size), self.current_query)
            )

    def to_dataframe(self):
        import pandas as pd

        return pd.DataFrame(
            self.records,
            columns=["timestamp", "object_key", "offset", "length", "file_size", "query_id"],
        )

    def to_csv(self, path: str):
        df = self.to_dataframe()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        df.to_csv(path, index=False)
        return df

    def reset(self) -> None:
        with self._lock:
            self.records.clear()
            self._t0 = time.time()


class _LoggingFile:
    """实现 Python 随机访问文件协议，并把每次 read 的 (offset,length) 记进 logger。

    pyarrow 的 C++ Parquet reader 对 NativeFile 发起 ReadAt(position,nbytes)，
    经 pa.PythonFile 转成本对象上的 seek()+read()，故在 read() 处即可捕获真实 range。
    """

    def __init__(self, path: str, object_key: str, logger: ReadLogger) -> None:
        self._fh = open(path, "rb", buffering=0)
        self._size = os.fstat(self._fh.fileno()).st_size
        self._key = object_key
        self._logger = logger

    # --- 读路径：记录访问 ---
    def read(self, nbytes: int | None = None) -> bytes:
        pos = self._fh.tell()
        data = self._fh.read() if (nbytes is None or nbytes < 0) else self._fh.read(nbytes)
        if data:
            self._logger.record(self._key, pos, len(data), self._size)
        return data

    def readinto(self, b) -> int:
        pos = self._fh.tell()
        n = self._fh.readinto(b)
        if n:
            self._logger.record(self._key, pos, n, self._size)
        return n

    # --- 随机访问支持 ---
    def seek(self, pos: int, whence: int = 0) -> int:
        return self._fh.seek(pos, whence)

    def tell(self) -> int:
        return self._fh.tell()

    def size(self) -> int:
        return self._size

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self._fh.close()

    @property
    def closed(self) -> bool:
        return self._fh.closed


class LoggingFileSystemHandler(pafs.FileSystemHandler):
    """包一层本地 FileSystem，读文件时返回带日志的输入流；其余操作透传。"""

    def __init__(self, logger: ReadLogger, base: pafs.FileSystem | None = None,
                 root: str | None = None) -> None:
        self._logger = logger
        self._base = base or pafs.LocalFileSystem()
        self._root = os.path.abspath(root) if root else None

    def _object_key(self, path: str) -> str:
        if self._root:
            try:
                return os.path.relpath(path, self._root)
            except ValueError:
                return path
        return os.path.basename(path)

    # --- 元信息 / 透传 ---
    def get_type_name(self):
        return "logging"

    def normalize_path(self, path):
        return self._base.normalize_path(path)

    def get_file_info(self, paths):
        return self._base.get_file_info(paths)

    def get_file_info_selector(self, selector):
        return self._base.get_file_info(selector)

    def create_dir(self, path, recursive=True):
        return self._base.create_dir(path, recursive=recursive)

    def delete_dir(self, path):
        return self._base.delete_dir(path)

    def delete_dir_contents(self, path, missing_dir_ok=False):
        return self._base.delete_dir_contents(path, missing_dir_ok=missing_dir_ok)

    def delete_root_dir_contents(self):
        return self._base.delete_dir_contents("", accept_root_dir=True)

    def delete_file(self, path):
        return self._base.delete_file(path)

    def move(self, src, dest):
        return self._base.move(src, dest)

    def copy_file(self, src, dest):
        return self._base.copy_file(src, dest)

    def open_output_stream(self, path, metadata=None):
        return self._base.open_output_stream(path, metadata=metadata)

    def open_append_stream(self, path, metadata=None):
        return self._base.open_append_stream(path, metadata=metadata)

    # --- 读：包成带日志的流 ---
    def open_input_stream(self, path):
        return pa.PythonFile(_LoggingFile(path, self._object_key(path), self._logger), mode="r")

    def open_input_file(self, path):
        return pa.PythonFile(_LoggingFile(path, self._object_key(path), self._logger), mode="r")

    def __eq__(self, other):
        return isinstance(other, LoggingFileSystemHandler) and other._root == self._root


def make_logging_fs(logger: ReadLogger, root: str | None = None) -> pafs.PyFileSystem:
    """构造可直接喂给 pyarrow.dataset / parquet 的 LoggingFileSystem。"""
    return pafs.PyFileSystem(LoggingFileSystemHandler(logger, root=root))
