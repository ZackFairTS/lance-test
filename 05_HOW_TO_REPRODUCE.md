# 如何复现压测

## 前置要求

- AWS EC2 或 EMR 实例（ARM64 或 x86_64 都行，测试用的 `r8g.2xlarge`）
- 实例所在 IAM role 有对目标 S3 bucket 的读写权限
- Docker 已安装（`docker` 命令可用）
- Maven + Git + curl 可用（或者用 dnf/apt 装一下）

## 总耗时

- 环境准备：~30 分钟（下 Flink, clone, build）
- 压测运行：每 run 12-15 分钟
- 结果分析：~5 分钟

## 步骤

### 1. 工作目录 + 基础工具

```bash
mkdir -p ~/lance-stress && cd ~/lance-stress
sudo dnf install -y git maven patchelf       # Amazon Linux
# 或 sudo apt install -y git maven patchelf  # Ubuntu
```

### 2. 下载 Flink 1.16.3

```bash
# Apache archive 很慢，用镜像
curl -sSL -o flink.tgz \
  "https://repo.huaweicloud.com/apache/flink/flink-1.16.3/flink-1.16.3-bin-scala_2.12.tgz"
tar -xzf flink.tgz && rm flink.tgz
```

### 3. Clone + Patch + Build lance-flink

```bash
git clone --depth 1 https://github.com/lance-format/lance-flink.git
cd lance-flink
```

#### 选择 lance-core 版本
编辑 `pom.xml`:
```xml
<lance.version>0.23.3</lance.version>   <!-- 默认，跟 connector 一致 -->
<!-- 或 -->
<lance.version>0.39.0</lance.version>   <!-- 最新 -->
<!-- 如果用 0.39.0，arrow 也要升: -->
<arrow.version>15.0.0</arrow.version>
```

#### Patch 1: S3 path check
```bash
sed -i 's|this.datasetExists = Files.exists(path);|this.datasetExists = true; // PATCH|' \
  src/main/java/org/apache/flink/connector/lance/LanceSink.java
```

#### Patch 2: read_version
手动编辑 `LanceSink.java` 找到 `// Append mode` 块（约 L186-189），改为：
```java
} else {
    // Append mode - PATCH: fetch current version, pass as read_version
    long readVersion;
    try (Dataset existing = Dataset.open(datasetPath, allocator)) {
        readVersion = existing.version();
    }
    FragmentOperation.Append append = new FragmentOperation.Append(fragments);
    dataset = append.commit(allocator, datasetPath, Optional.of(readVersion), Collections.emptyMap());
}
```

#### Patch 3 (仅 0.39.0): 移除过时的 IndexBuilder
```bash
rm src/main/java/org/apache/flink/connector/lance/LanceIndexBuilder.java
rm src/test/java/org/apache/flink/connector/lance/LanceIndexBuilderTest.java
rm src/test/java/org/apache/flink/connector/lance/LanceConnectorITCase.java
rm src/test/java/org/apache/flink/connector/lance/LanceVectorSearchTest.java
```

#### Fix Arrow shade relocation
`pom.xml` 的 `<relocations>` 加 excludes：
```xml
<relocation>
    <pattern>org.apache.arrow</pattern>
    <shadedPattern>org.apache.flink.connector.lance.shaded.arrow</shadedPattern>
    <excludes>
        <exclude>org.apache.arrow.c.**</exclude>
        <exclude>org.apache.arrow.dataset.**</exclude>
    </excludes>
</relocation>
```

#### Build
```bash
mvn clean package -DskipTests -q
ls -l target/*.jar   # 应该有 ~170-280MB 的 fat jar
cd ..
```

### 4. 构建 Docker 镜像

因为 lance-core 0.23.3 的 native lib 要求 GLIBC ≥ 2.35，Amazon Linux 2023 只有 2.34，需要用 Ubuntu 24.04 容器（0.39.0 可以在 AL2023 直接跑）。

把 `Dockerfile`, `scripts/`, `stress-job/` 从本 repo 下 `reproduction/` 目录复制过来，然后：

```bash
sudo docker build -t lance-flink-test:latest .
```

### 5. 构建测试作业

```bash
cd stress-job
mvn clean package -q
cd ..
```

### 6. 拿 AWS 凭证

```bash
# IAM role 自动拿 creds 注入 aws.env
TOKEN=$(curl -s -X PUT http://169.254.169.254/latest/api/token \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 3600")
ROLE=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/iam/security-credentials/)
CREDS=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  "http://169.254.169.254/latest/meta-data/iam/security-credentials/$ROLE")

python3 -c "
import json
c = json.loads('''$CREDS''')
open('aws.env','w').write(f'''
AWS_ACCESS_KEY_ID={c[\"AccessKeyId\"]}
AWS_SECRET_ACCESS_KEY={c[\"SecretAccessKey\"]}
AWS_SESSION_TOKEN={c[\"Token\"]}
AWS_REGION=ap-northeast-1
'''.strip())
"
chmod 600 aws.env
```

### 7. 启动 Flink 集群

```bash
sudo docker network create stressnet 2>/dev/null

# JobManager
sudo docker run -d --name jobmanager --network stressnet \
  -p 18081:8081 --env-file ./aws.env \
  -v "$(pwd):/workspace" \
  -e FLINK_PROPERTIES="$(cat <<'EOF'
jobmanager.rpc.address: jobmanager
jobmanager.memory.process.size: 2048m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4
rest.port: 8081
rest.bind-address: 0.0.0.0
state.backend: filesystem
state.checkpoints.dir: file:///tmp/flink-checkpoints
execution.checkpointing.interval: 10s
execution.checkpointing.timeout: 60s
execution.checkpointing.tolerable-failed-checkpoints: 0
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 20
restart-strategy.fixed-delay.delay: 5s
EOF
)" lance-flink-test:latest jobmanager

# TaskManager
sudo docker run -d --name taskmanager --network stressnet \
  --env-file ./aws.env -v "$(pwd):/workspace" \
  -e FLINK_PROPERTIES="$(cat <<'EOF'
jobmanager.rpc.address: jobmanager
taskmanager.numberOfTaskSlots: 4
taskmanager.memory.process.size: 4096m
state.backend: filesystem
state.checkpoints.dir: file:///tmp/flink-checkpoints
EOF
)" lance-flink-test:latest taskmanager

sleep 10
curl -s http://localhost:18081/overview | python3 -m json.tool
```

### 8. 运行压测

改你的 bucket 名，然后：

```bash
export S3_BUCKET=your-test-bucket
export RUN_ID=$(date +%Y%m%d-%H%M%S)
export DATASET_PATH="s3://$S3_BUCKET/stress-test/$RUN_ID/dataset"

# 预建表（parallelism=1 overwrite 一张空表，避免 Bug 1 触发）
sudo docker run --rm --env-file aws.env -v "$(pwd):/workspace" \
  lance-flink-test:latest \
  python3 /workspace/scripts/create_dataset.py "$DATASET_PATH"

# 运行压测
PHASE1_SECONDS=120 PHASE2_SECONDS=420 PHASE3_SECONDS=120 \
RATE_TOTAL=10000 PARALLELISM=4 BATCH_SIZE=1024 \
COMPACTOR_SLEEP=2 \
bash scripts/run_stress.sh main-$RUN_ID
```

### 9. 查看结果

```bash
cd logs/main-$RUN_ID

# checkpoint 失败数
cat checkpoints.json | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('Counts:', d['counts'])
print('CP dur:', d['summary']['end_to_end_duration'])
"

# restart 次数和原因
cat exceptions.json | python3 -c "
import json,sys
d=json.load(sys.stdin)
entries=d['exceptionHistory']['entries']
print(f'Exceptions: {len(entries)}')
for e in entries[:5]:
    print(' -', e['stacktrace'].split(chr(10))[0][:150])
"

# 数据重复率
sudo docker run --rm --env-file ../../aws.env lance-flink-test:latest \
  python3 -c "
import lance, pyarrow.compute as pc
ds = lance.dataset('$DATASET_PATH')
tbl = ds.to_table(columns=['id'])
total = len(tbl); uniq = len(pc.unique(tbl['id']))
print(f'Rows: {total}  Unique: {uniq}  Dup: {total-uniq}  ({100*(total-uniq)/total:.1f}%)')
"
```

## 可调参数（压力档位）

修改 `run_stress.sh` 或环境变量：

| 参数 | 轻 | 中 | 重 |
|---|---|---|---|
| `RATE_TOTAL` | 1,000 | 10,000 | 50,000 |
| `PARALLELISM` | 1 | 4 | 8 |
| `BATCH_SIZE` | 65536 | 1024 | 100 |
| `COMPACTOR_SLEEP` | 60 | 2 | 0 |
| `checkpoint.interval` | 300s | 10s | 2s |

**想重现我的结果**：用 "中" 档。
**想轻松复现触发 restart**：用 "重" 档。
**想证明 well-tuned 能避免 restart**：用 "轻" 档。

## 常见坑

### GLIBC 版本不够
症状：`version 'GLIBC_2.38' not found (required by liblance_jni.so)`
解决：压测跑在 Ubuntu 24.04 Docker 里，不要在 AL2023 宿主上直接跑（除非用 lance-core 0.39.0 它只要 2.28）。

### `Paths.get("s3://...")` bug
症状：parallelism > 1 时数据大面积丢失，没有异常
解决：必须打 Patch 1 + 预建表。

### `read_version must be specified`
症状：`IllegalArgumentException: Invalid user input: read_version must be specified`
解决：必须打 Patch 2。

### AWS IAM credentials 过期
症状：S3 403 Forbidden
解决：EMR role 的 session token 一般 6 小时过期。每次压测前重新拉 creds 写入 `aws.env`。脚本 `run_stress.sh` 开头会自动刷新。

### Flink 集群启动失败
症状：`docker logs jobmanager` 显示 bind port 失败
解决：`sudo docker ps -a --filter name=jobmanager -q | xargs sudo docker rm -f`

## 参考实际 run ID

本仓库 `data/` 下有两次完整 run 的原始输出：
- `data/run1_lance_0.23.3/` — RunID `main-v2-064711`, 2026-04-28
- `data/run2_lance_0.39.0/` — RunID `main-v39-074627`, 2026-04-28
