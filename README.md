# **README** - FINANCE_for_YP

建议在自己的 Docker 环境中先安装好 Maven（`lifecycle/build.py` 会直接调用 `mvn`）；如果你使用 IntelliJ 打包，请确保生成的 jar 路径/名称与 `lifecycle/launch.py` 里配置的 `DEFAULT_JAR_PATH` 一致。

本仓库的“可运行流水线”由 `lifecycle/` 下 3 个 Python 脚本组成：构建 Jar → 启动 Hadoop 作业并拉回本地 → 校验输出（可画误差分布图）。



## 需要你按自己环境修改的配置

`lifecycle/launch.py` 内置默认值如下（可通过环境变量覆盖，也可以直接改源码常量）：

```python
DEFAULT_JAR_PATH = "factor-mapreduce/target/factor-mapreduce-0.1.0-SNAPSHOT.jar"
DEFAULT_MAIN_CLASS = "factor.Driver"
DEFAULT_HDFS_INPUT_ROOT = "/user/pogi/HD_INPUT_REPO/FINANCE_for_YP"
DEFAULT_HDFS_OUTPUT_ROOT = "/user/pogi/HD_OUTPUT_REPO/FINANCE_for_YP"
DEFAULT_LOCAL_OUT_DIR = "local_buffer/hdfs_out"
```

你需要把其中的 HDFS 根路径（以及必要时的用户名/仓库名）改成你自己集群上的实际位置。



## **Pipeline**（3个.py脚本）

### 1) 构建：`lifecycle/build.py`
- 作用：在 `factor-mapreduce/` 下执行 `mvn clean package`，生成 Hadoop 可运行的 jar。
- 用法：
  - `python lifecycle/build.py`
  - `python lifecycle/build.py --skip-tests`

### 2) 启动作业并拉回本地：`lifecycle/launch.py`
- 作用：
  - 计算/检查 HDFS 输入路径（支持按 `--day` 单天或全量）
  - 启动 `hadoop jar ... factor.Driver <input> <output>`
  - 作业完成后将 HDFS 输出 `-get` 到本地缓冲目录 `local_buffer/hdfs_out/`
- 常用用法：
  - 跑单天：`python lifecycle/launch.py --day 0102`
  - 跑全部天：`python lifecycle/launch.py`
  - 只做输入预检不启动：`python lifecycle/launch.py --day 0102 --dry-run`

### 3) 校验：`lifecycle/validate.py`
- 作用：
  - 读取标准答案（`std_ref/`）
  - 从 `local_buffer/hdfs_out/` 自动发现最新一次输出并对齐 `(day, tradeTime)` 做误差计算
  - 支持输出误差分布图到 `err_distr_out/`
- 常用用法：
  - 校验单天并画图：`python lifecycle/validate.py --day 0102 --plot both`
  - 校验全部天（不传 `--day`）：`python lifecycle/validate.py`


