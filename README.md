# **README** - FINANCE_for_YP

建议在自己的 Docker 环境中先安装好 Maven（`lifecycle/build.py` 会直接调用 `mvn`）；如果你使用 IntelliJ 打包，请确保生成的 jar 路径/名称与 `lifecycle/launch.py` 里配置的 `DEFAULT_JAR_PATH` 一致。

本仓库的“可运行流水线”由 `lifecycle/` 下 4 个 Python 脚本组成：构建 Jar → 启动 Hadoop 作业并拉回本地 → 校验输出 → 清理缓冲/输出目录。



## 需要你按自己环境修改的配置

`lifecycle/launch.py` 内置默认值如下（需要时请直接改源码常量）：

```python
DEFAULT_JAR_PATH = "POGI-ONE-RELEASE/target/POGI-ONE-RELEASE-0.1.0-SNAPSHOT.jar"
DEFAULT_MAIN_CLASS = (硬编码) "pogi_one.Driver"
DEFAULT_HDFS_INPUT_ROOT = "/user/pogi/HD_INPUT_REPO/FINANCE_for_YP"
DEFAULT_HDFS_OUTPUT_ROOT = "/user/pogi/HD_OUTPUT_REPO/FINANCE_for_YP"
DEFAULT_LOCAL_OUT_DIR = "local_buffer/hdfs_out"
```

你需要把其中的 HDFS 根路径（以及必要时的用户名/仓库名）改成你自己集群上的实际位置。



## **Pipeline**（4个.py脚本）

### 1) 构建：`lifecycle/build.py`
- 用法：
  - `python lifecycle/build.py`
  - `python lifecycle/build.py --test`
- 作用：
    - 在 `POGI-ONE-RELEASE/` 下执行 `mvn clean package`，生成 Hadoop 可运行的 jar。

### 2) 启动作业并拉回本地：`lifecycle/launch.py`
- 用法：
  - 直接运行：`python lifecycle/launch.py`
- 作用：
  - 检查 HDFS 输入根目录下是否存在 `*/*/snapshot.csv`
  - 启动 `hadoop jar <.jar> pogi_one.Driver <hdfs_in_dir> <hdfs_out_dir>`
  - 作业完成后将 HDFS 输出 `-get` 到本地缓冲目录 `local_buffer/hdfs_out/`

### 3) 校验：`lifecycle/validate.py`
- 用法：
  - `python lifecycle/validate.py`
- 作用：
  - 读取标准答案（`std_ref/`）
  - 从 `local_buffer/hdfs_out/` 自动发现最新一次输出并对齐 `(day, tradeTime)` 做误差计算

### 4) 清理输出与缓冲：`lifecycle/cleanbuf.py`
- 用法：
  - `python lifecycle/cleanbuf.py`
- 作用：
  - 删除 HDFS 输出根目录下所有 `run_*`
  - 删除本地 `local_buffer/` 下所有 `run_*` 子目录
