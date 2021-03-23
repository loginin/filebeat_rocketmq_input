package rocketmq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"testing"
	"time"
)

func TestRocketmqConsumerHello(t *testing.T) {
	endPoint := []string{"10.53.24.182:9876", "10.53.25.43:9876"}
	endPoint = []string{"10.4.27.13:9876"}
	c, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer(endPoint),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName("CID_filebeat"),
	)
	if err != nil {
		panic(err)
	}
	err = c.Subscribe("TopicTest", consumer.MessageSelector{},
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			fmt.Printf("subscribe callback: %v \n", msgs)
			return consumer.ConsumeSuccess, nil
		})
	if err != nil {
		panic(err)
	}
	err = c.Start()
	time.Sleep(time.Hour)
	c.Shutdown()
}
