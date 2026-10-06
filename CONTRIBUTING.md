# 贡献指南

[中文] **·** [English](CONTRIBUTING_en.md)

感谢关注 Binomic！本文档说明如何参与贡献。

## AI 协助政策

**本项目接受 AI 协助开发的代码。** 但请注意：

- 你（提交者）必须**认真审查**提交的每一行代码，无论它由你本人还是 AI 协助编写；
- AI 协助的代码与本人编写的代码**承担同等责任**——正确性、安全性、许可证合规
  均由提交者负责；
- 提交即视为你完全理解并认可该变更，"这是 AI 写的"不构成缺陷的免责理由。

## 开发环境

本项目使用 [uv](https://docs.astral.sh/uv/) 管理依赖，要求 Python ≥ 3.12。

```bash
git clone git@github.com:Ahsen17/binomic.git
cd binomic
make install    # 创建虚拟环境并安装依赖
make check      # lint + 类型检查 + 全部测试
```

常用命令：

| 命令 | 作用 |
|-|-|
| `make fix` | ruff 自动修复与格式化 |
| `make lint` | pre-commit + mypy（strict） |
| `make test` | 单元测试 |
| `make test-integration` | 集成测试（需要本地 Redis） |
| `make coverage` | 覆盖率报告（`fail_under = 80`） |

## 贡献流程

1. 在 [Issues](https://github.com/Ahsen17/binomic/issues) 中认领或提出问题；
2. Fork 仓库并从 `main` 创建特性分支，如 `feat/broker-metrics`；
3. 开发并补充测试（见下方"测试要求"）；
4. 确保本地 `make check` 通过；
5. 提交 Pull Request 到 `main`，CI（`pytest.yml`）必须全绿。

## 提交规范

提交信息遵循
[Conventional Commits](https://www.conventionalcommits.org/zh-hans/)：

```
<类型>(<范围>?): <描述>

<正文?>

<脚注?>
```

常用类型：`feat`、`fix`、`refactor`、`doc`、`perf`、`style`、`test`、`build`、
`chore`。CHANGELOG.md 由 [git-cliff](https://git-cliff.org) 依据提交历史自动
生成，**原则上禁止手动编辑**。

## 测试要求

- 测试目录镜像源码结构：`src/binomic/base/schemas.py` → `tests/base/test_schemas.py`；
- 通用 fixture 位于 `tests/conftest.py`，子模块 fixture 位于对应子模块的
  `conftest.py`；
- 用例统一组织为 `class TestXX`；
- 外部依赖策略：单测使用 fakexx（如 fakeredis）；无 fakexx 包的依赖用 async
  mock；集成测试（`test_integration/`）使用真实服务；
- 新增功能必须附带测试，覆盖率不得低于 80%。

## 行为准则

保持尊重与专业。对事不对人，欢迎新人提问。
