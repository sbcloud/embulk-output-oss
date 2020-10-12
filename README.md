# Aliyun Oss output plugin for Embulk

This file output plugin is used to write records to Aliyun OSS 

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: yes
* **Cleanup supported**: yes

## Configuration

- **endpoint**: OSS endpoint (string, default: `"http://oss-ap-northeast-1.aliyuncs.com"`)
- **accessKeyId**: Aliyun account access key id (string, required)
- **accessKeySecret**: Aliyun account access key secret (string, required)
- **bucket**: Target OSS bucket name (string, required)
- **filePath**: Target file path with file prefix under bucket, use default value will generate target files under root folder in bucket with names like tmpxxx (string, default: `"tmp"`)
- **fileExt**: Suffix of target file (string, required)
- **sequenceFormat**: Format for sequence part of target keys (string, default: `".%03d.%02d"`)
- **tmpPath**: Temporary file directory. If null, it is associated with the default FileSystem (string, default: `"null"`)
- **tmpPathPrefix**: Prefix of temporary files (string, default: `"embulk-output-oss-"`)
- **cannedAcl**: Canned access control list for created objects (enum, default: `"Default"`)


## CannedAccessControlList
You can choose one of below list:
- Default
- Private
- PublicRead
- PublicReadWrite

cf. https://www.alibabacloud.com/help/doc-detail/84838.htm

## Example

```yaml
out:
  type: oss
  endpoint: http://oss-ap-northeast-1.aliyuncs.com
  accessKeyId: XXXXXXXXXXXXXXXXX
  accessKeySecret: XXXXXXXXXXXXXXXXX
  bucket: embulk
  tmpPathPrefix: embulk-test
  filePath: tmp/tmp
  fileExt: .csv
  cannedAcl: PublicReadWrite
  formatter:
    type: csv
```


## Build

```
$ ./gradlew gem  
```
