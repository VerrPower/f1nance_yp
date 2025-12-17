package factor;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * 输出格式：每条记录只写 value（整行 CSV 文本）。
 *
 * <p>Hadoop 默认 {@code TextOutputFormat} 会写成 {@code key<TAB>value}，这会破坏项目要求的纯 CSV。
 * 因此 reducer 直接生成一整行 CSV，并用本 OutputFormat 仅输出 value。</p>
 */
public final class ValueOnlyTextOutputFormat extends FileOutputFormat<NullWritable, Text> {
    @Override
    public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext context) throws IOException {
        Path outputPath = getDefaultWorkFile(context, "");
        FileSystem fileSystem = outputPath.getFileSystem(context.getConfiguration());
        FSDataOutputStream fileOut = fileSystem.create(outputPath, false);
        DataOutputStream dataOut = new DataOutputStream(fileOut);
        return new RecordWriter<NullWritable, Text>() {
            @Override
            public void write(NullWritable key, Text value) throws IOException {
                if (value == null) {
                    return;
                }
                // 只写出 value 一行（由 reducer 拼好的 CSV 行）。
                dataOut.write(value.getBytes(), 0, value.getLength());
                dataOut.writeByte('\n');
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException {
                dataOut.close();
            }
        };
    }
}
