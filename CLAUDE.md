# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 核心概念

Quack 是一个带缓存和依赖关系的任务执行器，类似 Make 但专为现代开发流程设计。

**三个核心概念**：
- **Target**：可缓存的构建目标（如编译、测试），有输入依赖和输出产物
- **Script**：简单命令包装，用于执行 Target
- **Dependency**：依赖关系定义（源码、其他 Target、MySQL 表等）

**缓存机制**：
- 基于 checksum 的增量构建
- 四种缓存后端：`false`（禁用）/ `local`（本地）/ `cloud`（仅云端）/ `dev`（本地+云端）
- 支持云存储（OSS/S3）进行跨机器缓存共享

## 开发命令

### 测试
每次改代码必须执行三次：
```bash
uv run quack test                # 第一次：生成缓存
uv run quack test                # 第二次：验证缓存命中
uv run quack test --cache=false  # 第三次：验证无缓存也能通过
```

**禁止使用 `uv run pytest`**，必须使用 `uv run quack test`。

### 代码质量
```bash
uv run ruff check          # Lint 检查
uv run ruff format         # 格式化
uv run basedpyright        # 类型检查
```

### 常用 Quack 命令
```bash
quack --list              # 列出所有脚本
quack --list-all          # 列出所有脚本和 Target
quack <script-name>       # 执行脚本
quack <target-name>       # 执行 Target
quack --cache=false       # 禁用缓存执行
```

## 架构要点

### 配置系统
- `quack.yaml`：项目配置，定义 scripts/targets/dependencies
- `~/.config/quack/config.yaml`：用户全局配置
- 环境变量：`QUACK_*` 前缀，支持嵌套（`QUACK_CLOUD__REGION`）

### 模块结构
```
src/quack/
├── __main__.py        # CLI 入口
├── spec.py            # 解析 quack.yaml，管理 Target/Script
├── config.py          # 配置管理（Pydantic Settings）
├── cache.py           # 缓存系统（Local/Cloud/CI 三种后端）
├── cli.py             # Target/Script 执行逻辑
├── models/            # 核心模型
│   ├── target.py      # Target 定义和执行
│   ├── script.py      # Script 定义
│   ├── dependency.py  # 依赖类型（source/target/mysql）
│   └── command.py     # 命令执行封装
└── utils/
    ├── cloud.py       # 云存储客户端（OSS/S3）
    ├── checksummer.py # 计算文件 checksum
    └── archiver.py    # 归档/解压 tar.gz
```

### Target 执行流程
1. 解析 `quack.yaml`，构建依赖图
2. 计算 Target checksum（基于依赖的 source/target/mysql）
3. 查找缓存：本地 → 云端（如果是 dev 模式）
4. 缓存未命中：执行 build 命令，生成产物，保存缓存
5. 缓存命中：直接解压产物到工作目录

### 云存储 (cloud.py)
- 统一接口支持 OSS 和 S3

## 关键实现细节

### 依赖类型
- `source`：源码文件（支持正则匹配和 exclude）
- `target`：其他 Target（自动管理执行顺序）
- `command`：命令输出作为依赖
- `variable`：环境变量作为依赖
- `global`：全局依赖（所有 Target 共享）

### 缓存路径
```
.quack-cache/<app-name>/<target-name>/<checksum>/
├── _metadata.json     # 元数据
└── _archive.tar.gz    # 产物归档
```
