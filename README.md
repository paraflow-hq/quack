
## 缓存安全与 R2 接入

缓存访问策略独立于缓存后端：

- `QUACK_CACHE_ACCESS=read_write`（默认）：允许在选定存储域读写缓存。
- `QUACK_CACHE_ACCESS=read`：命中、访问时间和 commit 索引均不写云；普通 miss 可本地重建，`--load-only` miss 失败。
- `QUACK_CACHE_ACCESS=none`：禁用缓存，不访问云端；`--load-only` 在联网前拒绝。`--cache=local/false` 也不会发布云索引。

客户端开关不构成权限隔离，reader 仍须使用存储侧只读身份。PR/开发凭据不得写可信构建和发布域。

R2 使用独立的缓存凭据，明确设置 `cloud.provider: r2`、`cloud.prefix: s3://<bucket>/<prefix>`、HTTPS R2 S3 endpoint 和 `cloud.region: auto`。R2 模式要求显式缓存 AK/SK，不能继承同进程 AWS 业务凭据；临时身份额外配置 `cloud.session_token`（环境变量 `QUACK_CLOUD__SESSION_TOKEN`）。不要把实际凭据写入源码或日志。

缓存格式 v2 使用 `.quack-cache-v2/<app>/` 和 `.quack-cache-dev-v2/<app>/`；本地目录按 endpoint、bucket/prefix、后端、`QUACK_CACHE_NAMESPACE` 和格式版本重新分域，不读取旧目录。此变更会冷构建，生产者与 `--load-only` 消费者必须协调升级并先生成新索引；不复制旧对象或回退到旧共享缓存。

新发布的提交索引使用格式 v3，引用 `_commit-archives/<sha256>.tar.zst` 中按内容寻址的归档。提交归档由本次工作区的输出生成，上传成功后才写入索引。旧 v2 提交索引仍能读取其按 Target 命名的归档，并继续验证其记录的摘要。普通缓存清理不会删除提交归档，存储生命周期应覆盖需要保留的部署和回滚窗口。

普通缓存按依赖 checksum 定位元数据，归档存为同一目录下的 `<sha256>.tar.zst`：生产者先上传归档再写元数据，读者先读元数据再下载其指向的归档；访问时间刷新只重新发布云端已有归档的元数据。同一 checksum 的多个生产者或访问时间刷新因此不会让元数据指向另一份归档。元数据指向的归档缺失时，普通模式按缓存损坏重新构建并重新发布，`--load-only` 失败。

恢复会核对索引 target/commit、归档 SHA256 和声明输出，拒绝越界、链接及特殊文件。输出必须位于工作区；归档内容不能自行扩大白名单。大小、展开量、单文件及条目数上限由 `QUACK_CACHE_LIMITS__` 前缀下的 `MAX_ARCHIVE_BYTES`、`MAX_EXPANDED_BYTES`、`MAX_FILE_BYTES`、`MAX_ENTRIES` 配置，默认分别为 2 GiB、8 GiB、2 GiB、100000。解码窗口另外限制为 128 MiB。路径、摘要、元数据及大小/条目数检查失败会直接报错；普通模式下，压缩流解码失败或临时云存储错误仍可重新构建，`--load-only` 则失败。

摘要检查不能认证生产者：若攻击者有权覆盖可信归档和索引，它仍能同时替换两者。正式发布还须限制可信写入身份、工作流/ref 与宿主，使用独立不可变发布物和来源记录。

CI 可使用 `QUACK_CONFIG_FILE` 指定工作区之外、权限为 `0600` 的临时配置文件；指定后不再合并用户全局或工作区 `.quack.yaml`，文件缺失时直接失败，不回退旧身份。文件的创建、job 结束删除和存储侧最小权限由调用流水线负责。
