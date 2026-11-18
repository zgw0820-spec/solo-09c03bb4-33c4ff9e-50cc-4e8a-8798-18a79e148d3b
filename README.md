# 离线制品分片续传服务

给制品库维护员使用的本地存储服务：浏览器选择离线镜像包、填写整包预期
SHA-256，服务按固定大小切分、逐片校验、断线后只补传缺片，全部核对通过后
才登记为可下载对象。FastAPI + SQLite + 本地持久目录，Docker Compose 一条
命令启动网页。

## 快速启动

```bash
docker compose up --build -d
# 打开 http://localhost:8000
```

数据保存在命名卷 `artifact-data`（容器内 `/data`）。常用环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `APP_PORT` | `8000` | 宿主机端口 |
| `CHUNK_SIZE` | `8388608`（8 MiB） | 新会话的分片大小；已建会话沿用记录值 |
| `DATA_DIR` | `/data` | 持久目录 |
| `WEB_CONCURRENCY` | `1` | uvicorn 工作进程数；>1 也安全（会话级 flock + WAL） |

不使用 Docker 时：

```bash
python3 -m pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --port 8000
```

## 页面操作流程

1. 选择镜像包文件，填写整包 SHA-256（64 位十六进制），点“开始上传 / 继续上传”。
   页面显示**上传会话号**、分片数和进度。
2. 断线/关页后重新打开页面：页面会提示上次未完成的会话。**重新选择同一文件**
   （同名同大小，摘要自动回填），点同一个按钮即可。客户端先调用
   `GET /api/uploads/{id}` 查询已确认分片，只补传缺失索引，然后自动完成校验。
3. “停止”可随时中断；再次点击继续不受影响。分片上传失败（网络错误/5xx）会
   自动指数退避重试 4 次。
4. 全部完成后出现“下载”按钮。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/` | 操作页面 |
| `GET` | `/api/health`、`/api/config` | 健康检查、分片大小 |
| `POST` | `/api/uploads` | 创建/复用会话（表单：`filename`、`file_size`、`expected_sha256`）。同名同大小同摘要且未完成时返回**原会话**及已收分片 |
| `GET` | `/api/uploads` | 按摘要/大小/文件名/状态查询会话 |
| `GET` | `/api/uploads/{id}` | 会话详情：`received_chunks[]`、`missing_chunks[]`、`status` |
| `PUT` | `/api/uploads/{id}/chunks/{idx}` | 上传分片（multipart 字段 `chunk`，可选头 `X-Chunk-SHA256`）。服务端按内容重算 SHA-256 |
| `POST` | `/api/uploads/{id}/complete` | 按序合并、逐片与整包校验、登记对象；幂等 |
| `GET` | `/api/uploads/{id}/download` | 仅已完成会话可下载 |
| `GET` | `/api/objects` | 已登记对象列表 |

错误统一为 `{"detail": {"code", "message", ...}}`，关键 `code`：
`chunk_digest_conflict`（同序号不同摘要）、`chunk_size_mismatch`、
`chunk_digest_mismatch`（`X-Chunk-SHA256` 与实际内容不符）、`chunks_missing`
（附 `missing_chunks`）、`digest_mismatch`（整包摘要不符）、
`upload_not_completed`、`object_unavailable`。

### 关键语义

* **分片摘要**：服务端自己计算分片 SHA-256，不信任客户端声明；请求头摘要仅用于
  额外核对，不一致直接 400。
* **同序号重传**：摘要一致 → 200，返回与首次相同的确认（服务端以原子替换写入
  同一文件，不产生副本）；摘要不同 → 409 `chunk_digest_conflict`，已确认分片
  保持不变。
* **重启续传**：每个已确认分片先 `fsync` 文件、`fsync` 目录，再写入 SQLite 行。
  重启后按数据库 + 磁盘文件恢复；若磁盘文件丢失/长度异常，该索引会被重新标记
  为缺失以便补传。
* **完成条件**：本会话分片序号连续、每片长度等于预期、逐片摘要与确认值一致、
  合并流的 SHA-256 等于整包预期值。任一不满足都不会登记对象（即使相同内容已被
  其他会话登记过，本会话也必须先收齐自己的分片）。
* **原子完成**：合并写入 `staging/`（隐藏、不可下载目录），逐片校验并 `fsync`
  后，若 `objects/<aa>/<sha256>` 已有相同且校验通过的文件则删除暂存副本复用它，
  否则原子改名到该路径；随后在**同一个 SQLite 事务**里插入 `objects` 行并把
  会话置为 `completed`。所以崩溃只可能留下“完整但未登记”的文件，绝不会暴露
  半成品；重试完成会重新合并校验并复用该文件。对象主键即内容摘要且 `sha256`
  唯一，并发/重复完成只登记一个对象。
* **并发**：分片提交与完成按会话加线程锁 + `flock` 文件锁，其他会话不受影响；
  SQLite 使用 WAL + `BEGIN IMMEDIATE` + `busy_timeout`。
* **登记前的隐藏性**：`GET /api/objects` 与下载都只认数据库登记；未登记的
  合并文件不会出现在任何接口中，服务重启时也不会自动登记。

### 数据布局

```
/data/service.sqlite3          uploads / chunks / objects 元数据（WAL）
/data/uploads/<session>/chunks/<idx>.part
/data/uploads/<session>/tmp/   请求体落盘暂存（超过 24h 的残留启动时清理）
/data/staging/                 已完成但未登记的合并文件
/data/objects/<aa>/<sha256>    已登记的最终对象
/data/locks/                   会话级 flock
```

## 测试

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest
```

14 个用例覆盖：断传后只补缺片、同序号重传幂等/冲突拒绝、分片长度与整包摘要
校验、磁盘分片丢失后重新标记缺失、已确认分片内容被篡改（同长度不同字节）时
完成被拒且不登记、进程重启后已确认分片续传、合并完成但登记前崩溃（重启后仍
不可下载，重试复用同一文件）、并发完成只产生一个对象且其他会话可继续上传、
相同内容的两个会话共享对象但各自仍需收齐分片、空文件、参数校验。

另外做过三类真实进程级验证，而非仅单元测试：

1. 启动 `uvicorn` 上传到一半后 `SIGKILL` 强杀；重启后创建/复用会话，服务端
   报 `received_count=2`、`missing=[1,3]`，补传后完成、下载内容摘要一致、重复
   完成返回同一对象。
2. `uvicorn --workers 2`（两个进程共享同一数据卷）：两个进程各自处理同一会话
   的不同分片、每片重复上传两次，再并发发起 4 次完成请求——4 次均 200，对象
   只有一个，下载摘要一致。
3. 用 jsdom 加载真实 `index.html` + `app.js` 对接真实服务：断传后重开页面、
   重新选择同一文件、只补传缺片并完成下载；错误摘要时页面显示
   `digest_mismatch` 且下载按钮保持隐藏，服务端无对象登记；未选文件点续传时
   给出明确提示而非静默失败。

未验证：Docker 镜像构建与 `docker compose up`（当前环境没有 Docker daemon），
仅对 `Dockerfile` / `docker-compose.yml` 做了静态审查；也没有在主流浏览器里做
人工点击验证（只有 jsdom 逻辑级验证，见上）。

## 已知限制

* 无鉴权、无 HTTPS：面向内网/单机维护场景，请置于可信网络或反向代理之后。
* 会话与分片不会被自动过期回收；已完成会话的分片文件在完成后删除，元数据保留。
  如需清理请按 `created_at` 自行处理。
* 单请求体上限 64 MiB（`MAX_CHUNK_BYTES`），即分片大小上限；如需更大分片需同时
  调整该常量与 `CHUNK_SIZE`。
* 下载为直接文件响应，不支持 HTTP Range 断点下载；续传能力针对上传。
* 同一文件名、大小、摘要的**未完成**会话会被复用（这是断线续传的设计），因此
  无法并行开两个相同文件的上传会话。
