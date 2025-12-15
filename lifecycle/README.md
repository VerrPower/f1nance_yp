# lifecycle

本目录包含本项目的“生命周期脚本”（纯 Python）：

- `lifecycle/build.py`：mvn clean + 打包 jar（删除旧 jar，生成新 jar）
- `lifecycle/launch.py`：运行 Hadoop Jar，并自动将 HDFS 输出拷回本地 `local_buffer/hdfs_out/`
- `lifecycle/validate.py`：对比标准答案（优先 `std_red/`，否则 `std_ref/`）进行准确性校验，并可输出误差分布图到 `err_distr_out/`
