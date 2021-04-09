package rocketmq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"os"
	"testing"
	"time"
)

func init() {
	os.Setenv("ROCKETMQ_GO_LOG_LEVEL", "warn")
}

func TestRocketmqConsumerHello(t *testing.T) {
	endPoint := []string{"10.53.24.182:9876", "10.53.25.43:9876", "10.53.25.78:9876"}
	//endPoint = []string{"10.4.27.14:9876"}
	c, err := rocketmq.NewPushConsumer(
		//consumer.WithNamespace("aix-mq"),
		consumer.WithNameServer(endPoint),
		//consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName("CID_filebeat"),
	)
	rlog.SetLogLevel("warn")
	if err != nil {
		panic(err)
	}
	err = c.Subscribe("filebeat", consumer.MessageSelector{},
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			fmt.Errorf("subscribe callback: %v \n", msgs)
			return consumer.ConsumeSuccess, nil
		})
	if err != nil {
		panic(err)
	}
	err = c.Start()
	time.Sleep(time.Hour)
	c.Shutdown()
}
