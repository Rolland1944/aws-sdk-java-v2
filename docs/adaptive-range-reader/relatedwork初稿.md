# Related Work 初稿

## 写作说明（审校时可删）

- 结构：四类工作，每段最后一到两句给出与 RECAS 的区分点。正文为英文 LaTeX，可直接粘进 `relatedwork.tex`。
- 引用：只使用仓库 `.tex` 中已出现过的 cite key。凡是标注 `\cite{TODO:...}` 的，均为**建议补充、尚未核实**的文献，请逐一核对原文后再决定是否保留。
- 不重复 Motivation 中的测量数字（如 68× 读放大、23.6 ms / 81 ms），需要时用 `Section~\ref{sec:moti}` 回指。
- 缓存策略（W-TinyLFU、W-TinyLFU-AV、SIEVE、SLRU 等）已在 evaluation 中作为 baseline 讨论，本节不再展开，只比较缓存系统。

---

## LaTeX 正文

```latex
\section{Related Work}
\label{sec:related}

\noindent\textbf{I/O optimization at the object storage access layer.}
A line of work optimizes how analytical systems retrieve data from object
storage. AnyBlob~\cite{anyblob} replaces the vendor SDK with an
io\_uring-based download manager and derives guidelines for request sizing and
concurrency, so that each retrieval achieves high throughput at low CPU cost.
Hadoop S3A provides a prefetching input stream backed by a fixed-size block
cache~\cite{s3aprefetch}, and columnar readers increasingly issue vectored
reads that coalesce nearby column-chunk ranges into fewer, larger
requests~\cite{TODO:hadoop-vectored-io,TODO:arrow-read-range-cache}.
These techniques either reduce the cost of each request or reshape the
requests of a single reader according to static rules. RECAS is orthogonal
to AnyBlob: it targets the \emph{number} of remote requests rather than the
efficiency of each one, and the two can be combined. Unlike reader-specific
prefetching and range merging, RECAS sits beneath all readers, decouples
fetch granularity from cache granularity, and adapts how each request is
issued to the access pattern observed in the request stream.

\noindent\textbf{Caching for cloud analytics.}
Caching is a standard remedy for the latency of remote storage, and existing
systems place it at different layers. Snowflake caches table data on the local
disk of each worker node: a node retains the file headers and the individual
columns it has read, and replaces them under a least-recently-used policy that
is oblivious of individual queries~\cite{snowflake}. Spark caches computed
RDDs in memory~\cite{rdd}, which reuses results rather than remote bytes.
Engine-agnostic caching systems make cached data shareable across engines, but
each comes with its own deployment path. Alluxio~\cite{TODO:alluxio}
interposes a separate distributed service that exports a unified namespace
through an HDFS-compatible interface, so caching is transparent to the
application but every engine must reach its data through that service.
Crystal~\cite{TODO:crystal} instead runs as a process co-located with each
compute node, driven by a lightweight, DBMS-specific connector that hands it
the remote file path, the pushed-down predicates, and the columns a query
needs; the DBMS kernel is left unmodified, but a connector must be built for
every engine and the engine side must describe what it is about to read.
RECAS differs from these systems in three respects. First, \emph{where} it
caches: inside the object storage SDK that every engine and data loader
already links against, so it needs no separate service, no alternative
namespace or storage API, and no per-engine connector. Second, \emph{what} it
caches: the raw bytes of remote Range GETs, treating each request as one unit
of reuse, so that every hit removes an entire remote round trip rather than
serving part of a cached file, column, or computed result. Third, \emph{how}
it decides: RECAS neither relies on the engine to declare what it will read
nor admits data by recency alone; it infers from the request stream it
observes which requests recur, how costly they are to refetch, and which
belong to one-shot large scans that should bypass the cache altogether
(Section~\ref{TODO:sec-cache}).

\noindent\textbf{Data layout optimization.}
Since the physical layout of a table determines how much data a query reads,
many systems tune it to the workload. Qd-tree~\cite{qdtree} learns a
partitioning tree from query predicates so that queries skip irrelevant
blocks. PTO~\cite{pto} jointly tunes partitioning, file size, row-group size,
and sort order for lakehouse tables, using query-level workload information
within Iceberg. Pixels~\cite{pixels} introduces its own columnar storage
format to support layout optimization for cloud data lakes. Table formats offer
compaction and clustering commands such as Iceberg's file rewriting and Delta
Lake's Z-ordering~\cite{iceberg,deltalake}, and physical design advisors such
as the DB2 Design Advisor~\cite{TODO:db2-advisor} separate candidate
generation from cost-based evaluation. Zeng et al.~\cite{zeng2023empirical}
characterize how the structural parameters of columnar formats affect
performance. These approaches rely on query predicates, a specific table
format, a new file format, or an optimizer's cost model. In contrast, RECAS
infers which columns are read, and which are read together, solely from the
byte ranges observed by the SDK and the Parquet footer. It prices candidate
layouts by replaying the reader's request-planning rules on a virtual footer,
with remote requests as the cost metric, and materializes the selected plan as
standard Parquet through the public writer. The rewritten files therefore
remain readable by every existing engine without modification.

\noindent\textbf{Reusing optimizations across engines.}
Lakehouse architectures decouple storage from compute so that many engines
share the same open table and file formats~\cite{lakehouseCompare,iceberg,hudi,deltalake}.
This openness, however, means that I/O optimizations remain embedded in each
engine and cannot be reused by others reading the same
files~\cite{activedatalakes}. Shared execution libraries such as
Velox~\cite{TODO:velox} and Arrow~\cite{TODO:arrow} reuse operator and reader
implementations across engines, but each engine must still be ported to them.
RECAS takes a complementary route: it places optimizations in the object
storage SDK, the one component that every engine and data loader already
depends on. As a result, they apply to SQL and non-SQL workloads alike without
modifying any engine.
```

---

## 各段要点与待办（中文）

### 1. 对象存储访问层 / SDK 层
- **区分点**：AnyBlob 降低每次请求的开销，RECAS 减少请求次数，两者正交、可以叠加；S3A 和 Parquet reader 的优化只作用于单个 reader，且使用静态规则。
- **待办**：
  - 核实 AnyBlob 原文中"请求大小 / 并发建议"的具体表述；
  - 补充 Hadoop vectored IO（HADOOP-18103）和 Arrow `ReadRangeCache` 的引用；
  - 等 intro 中 "Access-pattern-aware I/O" 那段补完后，同步调整本段最后一句。

### 2. 缓存
- **区分点**（只和缓存系统比较）：
  - where：缓存放在 SDK 内部，不另起服务、不换命名空间或存储 API、不需要为每个引擎写 connector；
  - what：缓存的是远程 Range GET 的原始字节，以整条请求为复用单位，而不是文件、列或计算结果；
  - how：只依据观察到的请求流来决定（是否重复出现、重新获取的代价、是否属于应当绕过缓存的一次性大扫描），既不依赖引擎声明要读什么，也不是只看 recency。
- **已核实**（依据用户提供的原文）：
  - Snowflake 缓存的是 **file headers + 单个列**，不是整文件；位于 worker 节点本地磁盘；LRU 且 oblivious of individual queries。初稿原先写成"caches table files"，已改正；
  - Alluxio 是**集群级独立服务**，统一命名空间 + HDFS 兼容接口，对应用透明，**不要求引擎声明缓存什么**；
  - Crystal 是**计算节点上的同机进程**，不是集群级服务；DBMS 内核不改，但必须接一个 DBMS-specific connector，由它传入文件路径、下推谓词和所需列。初稿原先把两者并列为"separate services or require the engine to describe"，已拆开分别描述。
- **待办**：
  - Snowflake 还用 consistent hashing 把文件集分配到节点以提升命中率，本段未提；如果审稿人可能追问"跨节点去重"，可补一句；
  - `\ref{TODO:sec-cache}` 需替换为 design 中 Caching 小节的 label。

### 3. 数据布局
- **区分点**：不依赖谓词、表格式、新文件格式或优化器代价模型；证据只有字节 trace + footer；以请求数作为代价；输出仍是标准 Parquet。
- **待办**：
  - 核实 PTO 的四个参数和"依赖 Iceberg"的表述（依据是 `TRACK2_PLAN.md` / `阶段汇报_Track2.md` 中的记录）；
  - Pixels 的一句话描述请对照原文；
  - 是否加入 ATUN-HL、SIGMOD'17 宽表列序（`TRACK2_PLAN.md` 第 9 节列出过），由你决定。如果加入，可以放在 DB2 Design Advisor 那一句后面。

### 4. 跨引擎复用
- **区分点**：Velox / Arrow 在库层面复用，但每个引擎都要移植；RECAS 放在每个引擎已经依赖的 SDK 中，无需修改引擎。
- **待办**：
  - 核实 `activedatalakes` 原文是否支持"引擎内优化无法复用"这一论断；
  - Velox / Arrow 这一句可选，用于提前回应"为什么不做成共享库"的质疑。
