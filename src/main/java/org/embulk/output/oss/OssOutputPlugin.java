package org.embulk.output.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.CannedAccessControlList;
import com.google.common.base.Optional;
import org.embulk.config.*;
import org.embulk.spi.*;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;


public class OssOutputPlugin
        implements FileOutputPlugin {

    public interface PluginTask
            extends Task {
        @Config("endpoint")
        @ConfigDefault("\"http://oss-ap-northeast-1.aliyuncs.com\"")
        public Optional<String> getEndpoint();

        @Config("accessKeyId")
        public String getAccessKeyID();

        @Config("accessKeySecret")
        public String getAccessKeySecret();

        @Config("bucket")
        public String getBucket();

        @Config("filePath")
        @ConfigDefault("\"tmp\"")
        public Optional<String> getFilePath();

        @Config("fileExt")
        String getFileNameExtension();

        @Config("sequenceFormat")
        @ConfigDefault("\".%03d.%02d\"")
        String getSequenceFormat();

        @Config("tmpPath")
        @ConfigDefault("null")
        Optional<String> getTempPath();

        @Config("tmpPathPrefix")
        @ConfigDefault("\"embulk-output-oss-\"")
        String getTempPathPrefix();

        @Config("cannedAcl")
        @ConfigDefault("\"Default\"")
        Optional<CannedAccessControlList> getCannedAccessControlList();
    }

    public static class OSSFileOutput implements FileOutput, TransactionalFileOutput {

        private final Logger log = Exec.getLogger(OssOutputPlugin.class);

        private final String bucket;
        private final String sequenceFormat;
        private final String fileNameExtension;
        private final String tempPathPrefix;
        private final Optional<CannedAccessControlList> cannedAccessControlListOptional;

        private int taskIndex;
        private int fileIndex;
        private OSS client;
        private String filePath = "/";
        private OutputStream current;
        private Path tempFilePath;
        private String tempPath = null;

        public static OSS newOSSClient(PluginTask task) {
            OSS client;

            if (task.getEndpoint().isPresent() && !task.getAccessKeyID().isEmpty() && !task.getAccessKeySecret().isEmpty()) {
                client = new OSSClientBuilder().build(task.getEndpoint().get(), task.getAccessKeyID(), task.getAccessKeySecret());
                return client;
            } else {
                throw new UnsupportedOperationException("Less parameters for oss output plugin");
            }
        }

        public OSSFileOutput(PluginTask task, int taskIndex) {
            this.taskIndex = taskIndex;
            this.client = newOSSClient(task);
            this.bucket = task.getBucket();
            if (task.getFilePath().isPresent()) {
                this.filePath = task.getFilePath().get();
            }
            this.sequenceFormat = task.getSequenceFormat();
            this.fileNameExtension = task.getFileNameExtension();
            this.tempPathPrefix = task.getTempPathPrefix();
            if (task.getTempPath().isPresent()) {
                this.tempPath = task.getTempPath().get();
            }
            this.cannedAccessControlListOptional = task.getCannedAccessControlList();
        }


        private static Path newTempFile(String tmpDir, String prefix)
                throws IOException {
            if (tmpDir == null) {
                return Files.createTempFile(prefix, null);
            } else {
                return Files.createTempFile(Paths.get(tmpDir), prefix, null);
            }
        }

        private void deleteTempFile() {
            if (tempFilePath == null) {
                return;
            }

            try {
                Files.delete(tempFilePath);
                tempFilePath = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private String buildCurrentKey() {
            String sequence = String.format(sequenceFormat, taskIndex,
                    fileIndex);
            return filePath + sequence + fileNameExtension;
        }

        private void putFile(Path from, String key) {
            try {
                InputStream inputStream = new FileInputStream(from.toString());
                client.putObject(bucket, key, inputStream);
                client.setObjectAcl(bucket, key, cannedAccessControlListOptional.get());
            } catch (Exception e) {
                throw new UnsupportedOperationException("Failed to put object with oss output plugin");
            }
        }

        private void closeCurrent() {
            if (current == null) {
                return;
            }

            try {
                putFile(tempFilePath, buildCurrentKey());
                fileIndex++;
            } finally {
                try {
                    current.close();
                    current = null;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    deleteTempFile();
                }
            }
        }

        @Override
        public void nextFile() {
            closeCurrent();

            try {
                tempFilePath = newTempFile(tempPath, tempPathPrefix);

                log.info("Writing OSS file '{}'", buildCurrentKey());

                current = Files.newOutputStream(tempFilePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void add(Buffer buffer) {
            if (current == null) {
                throw new IllegalStateException(
                        "nextFile() must be called before poll()");
            }

            try {
                current.write(buffer.array(), buffer.offset(), buffer.limit());
            } catch (IOException ex) {
                deleteTempFile();
                throw new RuntimeException(ex);
            } finally {
                buffer.release();
            }
        }

        @Override
        public void finish() {
            closeCurrent();
        }

        @Override
        public void close() {
            closeCurrent();
        }

        @Override
        public void abort() {
            deleteTempFile();
        }

        @Override
        public TaskReport commit() {
            TaskReport report = Exec.newTaskReport();
            return report;
        }
    }

    private void validateSequenceFormat(PluginTask task) {
        try {
            @SuppressWarnings("unused")
            String dontCare = String.format(Locale.ENGLISH,
                    task.getSequenceFormat(), 0, 0);
        } catch (IllegalFormatException ex) {
            throw new ConfigException(
                    "Invalid sequence_format: parameter for file output plugin",
                    ex);
        }
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount, Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        validateSequenceFormat(task);

        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, Control control) {
        control.run(taskSource);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports) {

    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, int taskIndex) {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        return new OSSFileOutput(task, taskIndex);
    }
}
