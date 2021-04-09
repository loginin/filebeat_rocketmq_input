```yaml
filebeat.inputs:
  - type: rocketmq
    consumer_model: 1
    name_server_addrs:
      - 10.4.27.13:9876
      - 10.4.27.14:9876
    group_name: "CID_filebeat"
    topic: "TopicTest"
```
### consumer_model:1表示集群模式

```yaml
filebeat.inputs:
  - type: rocketmq
    consumer_model: 1
    name_server_addrs:
      - 10.53.24.182:9876
      - 10.53.25.43:9876
      - 10.53.25.78:9876
    group_name: "CID_jimtopic"
    topic: "jimtopic"
    log_level: "debug"
output.file:
  path: "/Users/jim/Desktop/c/h"
  filename: "filebeat"
```
