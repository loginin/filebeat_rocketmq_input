# filebeat_rocketmq_input

# go.mod
```
require (
    github.com/apache/rocketmq-client-go/v2 v2.1.0
)
```

# filebeat > input
1. 新建rocketmq目录, 将config.go input.go拷贝
2. list.go加载rocketmq/input.go
```
    import _ "github.com/elastic/beats/v7/filebeat/input/rocketmq"
```

# 使用方法
```yaml
filebeat.inputs:
  - type: rocketmq
    name_server_addrs:
      - 10.4.27.13:9876
      - 10.4.27.14:9876
    group_name: "CID_filebeat"
    topic: "TopicTest"
```

# 代码提交历史
https://github.com/loginin/beats/commit/90f2dd0c7d02c22eb6ee8e68bef13e9319c22f3b