package rocketmq

import (
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"time"
)

type rocketMqInputConfig struct {
	NameServerAddrs []string      `config:"name_server_addrs" validate:"required"`
	GroupName       string        `config:"group_name" validate:"required"`
	Topic           string        `config:"topic" validate:"required"`
	WaitClose       time.Duration `config:"wait_close" validate:"min=0"`

	//路由选择
	Route        string `config:"route"` //sql92  tag
	RouteTag     string `config:"route_tag"`
	RouteExpress string `config:"route_express"`

	ConsumerModel int `config:"consumer_model"`

	ConsumeTimeout int32 `config:"consume_timeout"`

	ConsumeOrderly bool   `config:"consume_orderly"`
	FromWhere      string `config:"from_where"` //first, last, timestamp
	AutoCommit     bool   `config:"auto_commit"`

	RetryTimes   int    `config:"retry_times"`
	InstanceName string `config:"instance_name"`

	AccessKey     string `config:"access_key"`
	SecretKey     string `config:"secret_key"`
	SecurityToken string `config:"security_token"`
	Namespace     string `config:"namespace"`

	ConsumerPullTimeout int `config:"consumer_pull_timeout_mills"`

	// Concurrently max span offset.it has no effect on sequential consumption
	ConsumeConcurrentlyMaxSpan int

	// Flow control threshold on queue level, each message queue will cache at most 1000 messages by default,
	// Consider the {PullBatchSize}, the instantaneous value may exceed the limit
	PullThresholdForQueue int64 `config:"pull_threshold_for_queue"`

	PullThresholdSizeForQueue int `config:"pull_threshold_size_for_queue"`

	PullThresholdForTopic int `config:"pull_threshold_for_topic"`

	PullThresholdSizeForTopic int `config:"pull_threshold_size_for_topic"`

	PullInterval int `config:"pull_interval"`

	ConsumeMessageBatchMaxSize int `config:"consume_message_batch_max_size"`

	MaxReconsumeTimes int32 `config:"max_reconsume_times"`
}

type rocketmqConfig struct {
	opts        []consumer.Option
	topic       string
	msgSelector consumer.MessageSelector
}

func defaultConfig() rocketMqInputConfig {
	return rocketMqInputConfig{}
}

func newRocketmqConfig(ic rocketMqInputConfig) *rocketmqConfig {
	var opts []consumer.Option
	opts = append(opts, consumer.WithNameServer(ic.NameServerAddrs))
	if ic.AutoCommit {
		opts = append(opts, consumer.WithAutoCommit(ic.AutoCommit))
	}
	if ic.GroupName != "" {
		opts = append(opts, consumer.WithGroupName(ic.GroupName))
	}
	opts = buildConsumerModel(opts, ic)
	opts = buildFromWhere(opts, ic)
	if ic.ConsumeMessageBatchMaxSize > 0 {
		opts = append(opts, consumer.WithConsumeMessageBatchMaxSize(ic.ConsumeMessageBatchMaxSize))
	}
	return &rocketmqConfig{
		topic:       ic.Topic,
		msgSelector: genMsgSelector(ic),
		opts:        opts,
	}
}
func genMsgSelector(ic rocketMqInputConfig) consumer.MessageSelector {
	route := ic.Route
	if route == "tag" {
		return consumer.MessageSelector{
			Type:       consumer.TAG,
			Expression: ic.RouteTag,
		}
	} else if route == "sql92" {
		return consumer.MessageSelector{
			Type:       consumer.SQL92,
			Expression: ic.RouteExpress,
		}
	}
	return consumer.MessageSelector{}
}

func buildConsumerModel(opts []consumer.Option, ic rocketMqInputConfig) []consumer.Option {
	if ic.ConsumerModel == 0 {
		return append(opts, consumer.WithConsumerModel(consumer.BroadCasting))
	}
	return append(opts, consumer.WithConsumerModel(consumer.Clustering))
}
func buildFromWhere(opts []consumer.Option, ic rocketMqInputConfig) []consumer.Option {
	if ic.FromWhere == "first" {
		return append(opts, consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset))
	} else if ic.FromWhere == "timestamp" {
		return append(opts, consumer.WithConsumeFromWhere(consumer.ConsumeFromTimestamp))
	}
	return append(opts, consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset))
}
