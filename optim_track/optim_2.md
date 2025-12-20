# optim_2：Mapper 读取端“极限内联版”（去 Snapshot、去 String、全展开）

@ Driver: ElapsedSec: 62.727

目标：把 Map 端解析 + 因子计算的常数开销压到最低。核心思路：去除 Snapshot 封装、去除 Text->String、去除函数调用/分支树，所有字段解析与 20 因子计算都在 Mapper 热路径内联完成。

## 1) 改动摘要
- **去 Snapshot**：`StockFactorMapper` 不再构造 `Snapshot`，直接在 `map()` 内解析/计算。
- **byte[] 解析**：直接用 `Text.getBytes()/getLength()`，行尾仅裁一次 `CR`，全程不产生 `String`。
- **字段解析内联**：手写扫描指针 `pos`，跳字段/取字段全靠 `while`，无函数调用；非输出窗口行跳过 tBidVol/tAskVol 与因子计算，仅维护 `t-1` 状态。
- **全展开 level1~5**：前 5 档 bp/bv/ap/av 解析和加权求和全部手动展开，去掉循环与分支树。
- **倒数复用**：因子计算阶段预先算倒数，减少重复除法；`hasPrev` 是唯一分支。
- **固定 double**：因 float 版本误差超标，删除全部 float 通道，Driver 固定使用 double 管线。
- **log 模式精简**：`lifecycle/launch.py` 仅保留 `normal/mute`，去掉 redirect。

## 2) 涉及文件
- `POGI-ONE-RELEASE/src/main/java/pogi_one/StockFactorMapper.java`（核心内联改造）
- `POGI-ONE-RELEASE/src/main/java/pogi_one/Driver.java`（去浮点通道、固定 double）
- `POGI-ONE-RELEASE/src/main/java/pogi_one/Snapshot.java` & `src/test/java/pogi_one/SnapshotTest.java`（已删除）
- `lifecycle/launch.py`（log 模式裁剪）

## 3) 精度与风险
- double 通道验证通过（validate `form` 模式 alpha_14 之前的 float 误差超 1% 已规避）。
- 解析假设输入行首为 `YYYYMMDD,HHMMSS,`，且字段为纯数字整数；如输入格式变化需另行处理。

## 4) 验证方式
- 编译测试：`mvn -f POGI-ONE-RELEASE/pom.xml test`
- 构建：`mvn -f POGI-ONE-RELEASE/pom.xml -DskipTests clean package`
- 运行 + 校验：`python lifecycle/launch.py --day 0102 --log mute` 运行；`python lifecycle/validate.py --mode form` 校验。

## 5) 性能预期
- 去除 `String`/`Snapshot` 分配、函数调用与分支树后，Mapper CPU 常数显著下降，Shuffle 不变；适合高吞吐场景。进一步提速空间很小，需权衡可维护性。***
