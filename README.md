# 冷链温标补正入仓与可信检索 API（Cold-Chain Correction + Trusted Index）

冷链记录仪漏发/错发温标时，数据工程师通过本服务查询被阻塞的原始消息、补正核准温标（C/F），
由单工作进程事务性地写入清洁主题，避免摄氏/华氏混用导致超温误判。

冷链质控员按**箱号 + UTC 时段**追查已核准读数时，不必每次从头扫描清洁主题：手动触发增量
索引，将 `clean-readings` 双分区的已提交记录与各分区接续位置写入同一 SQLite，再通过带
**完整性判定**的查询 API 取得摄氏值与可定位的来源/清洁主题位置。部分结果绝不会被报成完整。

* **入仓**：FastAPI + `confluent-kafka` + 本地 Redpanda（Kafka 兼容）
* **主题**：固定双分区 `raw-readings`（源）与 `clean-readings`（清洁），清洁记录写入**同号分区**
* **进程模型**：单 uvicorn 工作进程、单消费者、固定 `transactional.id`
* **语义**：输出与源 offset 在**同一个 Kafka 事务**里提交；下游 `read_committed` 只见已提交记录；
  提交前后退出、重启、重复补正均不漏记、不重记、不越过阻塞消息
* **增量索引**：每次手动触发，从两个分区各读取**最多 50 条**已提交记录；读数与接续位置
  在同一 SQLite 事务落库，中断/重启/重复触发不漏读、不重记
* **可信查询**：查询开始即固定双分区已提交可读边界（low/high watermark）；索引未追齐、
  消息读取失败、采样时间无法解析或保留策略已删除未索引记录时，返回 `incomplete` 与可定位的
  进度/来源位置；保留缺口只报告、**不越过**

---

## 1. 目录结构

```
app/
  config.py        环境变量配置
  logic.py         纯校验/换算逻辑（C 直采，F=(r-32)*5/9）
  db.py            SQLite 持久层：阻塞消息 + 补正审计（WAL, synchronous=FULL）
  messaging.py     confluent-kafka 消费者/事务生产者/Topic 管理封装
  crash.py         可控进程退出注入（before_commit / after_commit）
  worker.py        单工作线程：消费、阻塞暂停、事务提交、重启对账
  clean_reader.py  read_committed 清洁记录读取器
  index_store.py   SQLite 索引持久层：读数、分区接续位置、可定位问题（WAL, synchronous=FULL）
  indexer.py       手动增量索引器：每次每分区最多 50 条，缺口/读取失败/时间解析问题定位
  timestamps.py    严格 UTC（ISO-8601，Z 或显式偏移）解析
  query.py         箱号+UTC 时段查询：查询开始固定双分区可读边界并给出完整性判定
  main.py          FastAPI 路由（/docs 可交互）
scripts/
  entrypoint.sh    等待 Redpanda 后以单 worker 启动 uvicorn
  seed.py          双分区确定性样例（每分区一个阻塞消息）
demo/
  reset.sh         清空容器与卷，恢复全新环境
  run_demo.py      提交前/提交后退出、重启、重复补正的可控演示
tests/             19 个测试（纯逻辑 + 内存 Kafka 替身的端到端语义测试）
docker-compose.yml redpanda + console + api + 一次性 seed
```

## 2. 快速启动

```bash
docker compose up -d --build
# 等待 API 就绪
curl -s http://localhost:8000/health
# 写入双分区样例
docker compose run --rm seed
# API 文档
open http://localhost:8000/docs
# Redpanda Console（可选）
open http://localhost:8080
```

### 索引与查询快速试用

```bash
# 1) 手动触发一次增量索引（每次每分区最多 50 条已提交记录，可重复触发追齐）
curl -s -X POST http://localhost:8000/index/run | python -m json.tool
# 2) 查看各分区接续游标与未决问题
curl -s http://localhost:8000/index/progress | python -m json.tool
# 3) 按箱号 + UTC 起止查询（status=complete 才可视为全量结果）
curl -s "http://localhost:8000/readings/query?box_id=BOX-A-003&start_utc=2026-11-13T00:00:00Z&end_utc=2026-11-14T00:00:00Z" \
  | python -m json.tool
```

服务端口：

| 服务 | 地址 |
| --- | --- |
| FastAPI / Swagger | http://localhost:8000/docs |
| Redpanda Kafka（宿主） | localhost:19092 |
| Redpanda Console | http://localhost:8080 |

SQLite 补正库位于命名卷 `api-data`（容器内 `/data/coldchain.db`），Redpanda 数据位于 `redpanda-data`。

## 3. 消息格式

### 原始消息（`raw-readings`，按显式分区写入）

```json
{"box_id": "BOX-A-003", "sampled_utc": "2026-11-13T08:00:20Z", "reading": 39.2, "scale": "F"}
```

* `scale="C"`：读数直接作为摄氏值
* `scale="F"`：摄氏值 = `(reading − 32) × 5/9`，原读数保留不改
* 缺字段、`scale` 缺失/为 null/非 `C|F`、读数非有限数值、JSON 非法 → 阻塞，不进清洁主题

### 清洁记录（`clean-readings`，同号分区、按源 offset 顺序）

```json
{
  "box_id": "BOX-A-003",
  "sampled_utc": "2026-11-13T08:00:20Z",
  "reading": 39.2,
  "scale": "F",
  "celsius": 4.0,
  "source_topic": "raw-readings",
  "source_partition": 0,
  "source_offset": 2
}
```

## 4. HTTP API（详见 `/docs`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 存活状态、当前暂停分区 |
| GET | `/blocked?status=blocked\|corrected\|committed` | 阻塞消息：原消息、分区、offset、原因、状态 |
| POST | `/blocked/{partition}/{offset}/correct` | 仅接受 `{"scale":"C"\|"F"}`；**不接受也无法修改原读数** |
| GET | `/clean?partition=&limit=` | 只读已提交清洁记录（`read_committed`） |
| GET | `/progress` | 各分区已提交源 offset（事务进度） |
| POST | `/index/run` | 手动触发一次增量索引（每分区最多 50 条已提交记录） |
| GET | `/index/progress` | 索引接续位置（游标）、状态与全部未决问题 |
| GET | `/readings/query?box_id=&start_utc=&end_utc=` | 按箱号 + UTC 起止查询，返回完整/未完成状态 |
| POST | `/demo/crash` | 武装一次性退出点 `before_commit` / `after_commit` |
| GET | `/demo/crash` | 查看武装状态 |
| POST | `/demo/resume/{partition}` | 演示用手动恢复（生产置 `AUTO_RESUME=1` 时补正即自动恢复） |

补正响应语义：

* 首次补正 → `200`，状态 `corrected`，立即落 SQLite
* 同值重复补正 → `200` 幂等忽略，审计表仍留痕
* 不同值重复补正 → `409` 冲突，已持久化的核准值不变
* 目标不存在 → `404`；已提交消息再补正 → `409`；温标非 C/F → `422`

## 5. 阻塞与补正如何工作

1. worker 按 offset 顺序消费双分区。
2. 命中无效消息：原消息（或原始字节解析失败信息）、分区、offset、原因写入 SQLite，
   **仅暂停该分区**；另一分区继续消费并提交。
3. 工程师在 `/docs` 或 API 提交 C/F 补正 → SQLite（WAL + `synchronous=FULL`）确认持久后，
   分区才允许恢复。
4. 恢复后 Kafka 重投该 offset：worker 读取持久化的核准温标，用**原读数**换算并产出清洁记录。
5. 清洁记录与“源 offset+1”在**同一事务**提交，随后 SQLite 行标记为 `committed`。

### 为什么崩溃也不会漏记/重记/跳过

| 崩溃点 | 持久化状态 | 重启行为 |
| --- | --- | --- |
| 补正落库后、事务提交前退出（`before_commit`） | SQLite=`corrected`，Kafka 无输出、offset 未动 | 启动对账保留核准温标；消息重投，清洁记录恰好产出一次 |
| 事务提交后、SQLite 标记前退出（`after_commit`） | 清洁记录与 offset 已原子可见，SQLite 仍 `corrected` | 已提交 offset 不会重投；无重复产出 |
| 事务中止/未提交 | 所有产出对 `read_committed` 不可见 | 消息重投，无脏数据 |

固定 `transactional.id` 提供僵尸 fencing：旧生产者纪元的未决事务在新实例
`init_transactions()` 时被中止，单进程模型保证不会有两个消费者同时占用分区。

## 6. 增量索引与可信查询

### 6.1 手动增量索引 `POST /index/run`

每次触发执行一个**有界**索引批次：

1. 新建 `read_committed` 消费者（独立匿名 group，从不提交 group offset，不影响入仓 worker）；
2. 批次开始时记录两个分区的 low/high watermark 作为本次可读边界；
3. 每个分区从 SQLite 中的接续游标（`index_progress.next_offset`）开始，最多读取
   **50 条**已提交记录（未提交/已中止事务的记录不可见）；
4. 每条读数与“游标推进到 offset+1”在**同一个 SQLite 事务**提交，原温标、原读数、
   摄氏补正值、来源位置（`source_topic/partition/offset`）与清洁主题位置
   （`clean_partition/clean_offset`）一并落库。

响应示例：

```json
{
  "clean_topic": "clean-readings",
  "batch_size": 50,
  "indexed_total": 12,
  "complete": false,
  "partitions": [
    {"partition": 0, "low_watermark": 0, "high_watermark": 60,
     "start_offset": 50, "indexed": 10, "next_offset": 60, "complete": true, "issues": []},
    {"partition": 1, "low_watermark": 0, "high_watermark": 2,
     "start_offset": 0, "indexed": 2, "next_offset": 2, "complete": true, "issues": []}
  ],
  "issues": []
}
```

`complete=false` 表示至少一个分区在批次结束时仍有已提交可读记录未索引（达到 50 条上限、
遇到缺口或读取失败）；再次触发即可继续。并发触发返回 `409`（同一时刻只允许一个批次）。

### 6.2 为什么中断、重启、重复触发不漏读、不重记

* 游标只在与读数插入**同一事务**中前进：批次中途进程退出，已提交的行与游标一致，
  未处理的 offset 游标不动，重启后从旧游标重读——恰好一次。
* 读数按 `(clean_topic, clean_partition, clean_offset)` 作主键并 `ON CONFLICT DO NOTHING`：
  重复触发不会产生重复行。
* 索引消费者使用独立 group 且不提交 offset，与入仓 worker 的消费组完全隔离。

### 6.3 查询 `GET /readings/query`

参数：`box_id`（精确匹配）、`start_utc`、`end_utc`（含端点，ISO-8601 且必须带 `Z` 或
显式偏移；朴素时间戳返回 `422`）、可选 `limit`。

* 查询**开始时**固定两个分区的已提交可读边界（low/high watermark）；
* 返回按采样时间、再按来源位置排序的读数，含摄氏值、原温标/原读数、来源分区/offset 与
  清洁主题分区/offset；
* `status` 为 `complete` 当且仅当：索引游标已追平查询开始时的 high watermark、不存在
  未决缺口/读取失败，且该箱号没有采样时间无法解析的记录。否则为 `incomplete`，
  仍返回已索引的匹配行，但同时给出可定位信息，绝不把部分结果报成完整。

```json
{
  "status": "incomplete",
  "box_id": "BOX-A-003",
  "count": 1,
  "readings": [
    {"box_id": "BOX-A-003", "sampled_utc": "2026-11-13T08:00:20Z", "celsius": 4.0,
     "scale": "F", "reading": 39.2, "source_topic": "raw-readings",
     "source_partition": 0, "source_offset": 2,
     "clean_partition": 0, "clean_offset": 2}
  ],
  "boundaries": [
    {"partition": 0, "low_watermark": 0, "high_watermark_at_query_start": 60}
  ],
  "index_progress": [
    {"partition": 0, "low_watermark": 0, "high_watermark_at_query_start": 60,
     "indexed_next_offset": 50, "status": "active"}
  ],
  "issues": []
}
```

### 6.4 未完成状态与问题定位（`issues` / `index_progress`）

| `kind` | 触发条件 | 定位字段 | 系统行为 |
| --- | --- | --- | --- |
| `index lag`（隐式，见 `index_progress`） | 游标 < 查询开始时 high watermark | `partition`、`indexed_next_offset`、`high_watermark_at_query_start` | 返回 `incomplete`；继续触发 `/index/run` 追齐 |
| `retention_gap` | 保留策略已删除游标之前、尚未索引的记录 | `partition`、`detail.missing_from`、`detail.low_watermark` | 分区状态置 `gap` 并**停在缺口**，不越过；重复触发保持停放 |
| `read_failure` | Kafka 读取报错或持续空轮 | `partition`、`clean_offset`（待读 offset）、`detail.error` | 游标不前进；下次触发重试，成功后问题自动解除 |
| `unparseable_sampled_utc` | 读数已索引但采样时间无法解析 | `partition`、`clean_offset`、`box_id`、`detail.sampled_utc` | 读数不丢，但无法参与时段比较；相关箱号查询返回 `incomplete` |
| `unreadable_record` | 清洁消息本身非法 JSON/缺字段 | `partition`、`clean_offset` | 停在该 offset，不盲目跳过 |
| `boundary_unavailable` | 查询时无法取得可读边界 | `detail.error` | 返回 `incomplete`，同时给出 SQLite 中已持久化的游标进度 |

`GET /index/progress` 随时返回各分区游标、`active/gap` 状态、最近一次批次见到的
high watermark 与全部未决问题。

## 7. 可控演示

```bash
# 一键完整演示（内部自动 reset 两次，分别覆盖两个崩溃点）
python demo/run_demo.py full

# 或分步执行
demo/reset.sh
python demo/run_demo.py A      # 提交前退出 -> 重投 -> 恰好一次
demo/reset.sh
python demo/run_demo.py B      # 提交后退出 -> 不重投 -> 无重复
```

演示会断言：阻塞分区独立暂停、同号分区顺序、原值保留、重复补正幂等/冲突、
清洁表无重复源 offset、进度与输出一致。

## 8. 测试

本仓库在无 Docker 的环境中使用内存版 `confluent_kafka` 替身（`tests/fake_kafka`，
实现事务、offset 事务提交、暂停/恢复、`read_committed` 隔离、生产者 fencing，以及
low/high watermark 与保留删除）验证语义：

```bash
python -m pytest tests/ -q
# 35 passed
```

覆盖：C/F 换算、各类无效消息阻塞、单分区暂停不影响另一分区、补正后顺序、
重复补正幂等与冲突、提交前/提交后退出重启的恰好一次、中止事务不可见；以及增量索引的
50 条/分区批次边界、重复触发幂等、重启接续、中途中断无漏读/重记、读取失败游标不前进、
采样时间不可解析可定位、保留缺口报告且不越过、并发触发 409、查询边界固定与完整性判定。

## 9. 配置项

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `redpanda:9092` | Kafka/Redpanda 地址 |
| `RAW_TOPIC` / `CLEAN_TOPIC` | `raw-readings` / `clean-readings` | 双分区主题 |
| `TOPIC_PARTITIONS` | `2` | 固定分区数 |
| `GROUP_ID` | `coldchain-worker` | 单消费者组 |
| `TRANSACTIONAL_ID` | `coldchain-txn-1` | 固定事务 ID（fencing） |
| `SQLITE_PATH` | `/data/coldchain.db` | 补正与索引共用的 SQLite 文件 |
| `INDEX_BATCH_SIZE` | `50` | 每次 `/index/run` 每分区最多读取的已提交记录数 |
| `INDEX_POLL_TIMEOUT` | `0.5` | 索引消费者单次轮询超时（秒） |
| `AUTO_RESUME` | `0`（compose） | 补正后是否自动恢复分区；生产建议 `1` |
| `CRASH_ON` | 空 | 启动即武装的退出点（一般用 API 武装） |
