package factor;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * 禁止 Hadoop 切分输入文件的 InputFormat。
 *
 * <p>本项目的因子 alpha_17/alpha_18/alpha_19 依赖 “上一条快照（t-1）”。如果 Hadoop 将单个
 * CSV 文件切成多个 split，则后续 split 的第一条记录在 Mapper 中将无法拿到正确的上一条快照，
 * 造成因子计算错误。</p>
 *
 * <p>因此：强制每个股票文件作为一个整体由一个 Mapper 顺序读取处理。</p>
 *
 * <p>代价：降低了单个大文件的并行度；但对本项目（每股每天一个文件）通常是合理折中。</p>
 */
public final class NonSplittableTextInputFormat extends TextInputFormat {
    @Override
    protected boolean isSplitable(org.apache.hadoop.mapreduce.JobContext context, Path filename) {
        // 核心：永远不允许切分。
        return false;
    }
}
