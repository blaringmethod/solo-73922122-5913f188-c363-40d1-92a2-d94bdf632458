# 冷链温标补正入仓 API（Cold-Chain Scale Correction Intake）

冷链记录仪漏发/错发温标时，数据工程师通过本服务查询被阻塞的原始消息、补正核准温标（C/F），
由单工作进程事务性地写入清洁主题，避免摄氏/华氏混用导致超温误判。

* **入仓**：FastAPI + `confluent-kafka` + 本地 Redpanda（Kafka 兼容）
* **主题**：固定双分区 `raw-readings`（源）与 `clean-readings`（清洁），清洁记录写入**同号分区**
* **进程模型**：单 uvicorn 工作进程、单消费者、固定 `transactional.id`
* **语义**：输出与源 offset 在**同一个 Kafka 事务**里提交；下游 `read_committed` 只见已提交记录；
  提交前后退出、重启、重复补正均不漏记、不重记、不越过阻塞消息

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

## 6. 可控演示

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

## 7. 测试

本仓库在无 Docker 的环境中使用内存版 `confluent_kafka` 替身（`tests/fake_kafka`，
实现事务、offset 事务提交、暂停/恢复、`read_committed` 隔离与生产者 fencing）验证语义：

```bash
python -m pytest tests/ -q
# 19 passed
```

覆盖：C/F 换算、各类无效消息阻塞、单分区暂停不影响另一分区、补正后顺序、
重复补正幂等与冲突、提交前/提交后退出重启的恰好一次、中止事务不可见。

## 8. 配置项

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `redpanda:9092` | Kafka/Redpanda 地址 |
| `RAW_TOPIC` / `CLEAN_TOPIC` | `raw-readings` / `clean-readings` | 双分区主题 |
| `TOPIC_PARTITIONS` | `2` | 固定分区数 |
| `GROUP_ID` | `coldchain-worker` | 单消费者组 |
| `TRANSACTIONAL_ID` | `coldchain-txn-1` | 固定事务 ID（fencing） |
| `SQLITE_PATH` | `/data/coldchain.db` | 补正持久化文件 |
| `AUTO_RESUME` | `0`（compose） | 补正后是否自动恢复分区；生产建议 `1` |
| `CRASH_ON` | 空 | 启动即武装的退出点（一般用 API 武装） |
