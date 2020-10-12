Embulk::JavaPlugin.register_output(
  "oss", "org.embulk.output.oss.OssOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
