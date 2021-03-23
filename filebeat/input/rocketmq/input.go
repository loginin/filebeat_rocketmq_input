package rocketmq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/elastic/beats/v7/filebeat/channel"
	"github.com/elastic/beats/v7/filebeat/input"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/pkg/errors"
	"sync"
	"time"
)

func init() {
	err := input.Register("rocketmq", NewInput)
	if err != nil {
		panic(err)
	}
}

type rocketmqInput struct {
	config         rocketMqInputConfig
	rocketmqConfig *rocketmqConfig
	context        input.Context
	outlet         channel.Outleter
	log            *logp.Logger
	runOnce        sync.Once
	consumer       rocketmq.PushConsumer
}

func NewInput(
	cfg *common.Config,
	connector channel.Connector,
	inputContext input.Context,
) (input.Input, error) {
	config := defaultConfig()
	if err := cfg.Unpack(&config); err != nil {
		return nil, errors.Wrap(err, "reading rocketmq input config")
	}

	out, err := connector.ConnectWith(cfg, beat.ClientConfig{
		CloseRef:  doneChannelContext(inputContext.Done),
		WaitClose: config.WaitClose,
	})
	if err != nil {
		return nil, err
	}

	conf := newRocketmqConfig(config)

	p, err := rocketmq.NewPushConsumer(conf.opts...)
	if err != nil {
		return nil, err
	}

	err = p.Start()
	if err != nil {
		return nil, err
	}

	ri := &rocketmqInput{
		config:         config,
		rocketmqConfig: conf,
		context:        inputContext,
		outlet:         out,
		log:            logp.NewLogger("rocketmq input").With("hosts", config.NameServerAddrs),
		consumer:       p,
	}
	return ri, nil
}

func (input *rocketmqInput) Run() {
	input.runOnce.Do(func() {
		go func() {
			c := doneChannelContext(input.context.Done)
			for c.Err() == nil {
				err := input.consumer.Subscribe(input.rocketmqConfig.topic, input.rocketmqConfig.msgSelector,
					func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
						//fmt.Printf("subscribe callback: %v \n", msgs)
						//create Events
						events := input.createEvents(msgs...)
						for _, event := range events {
							input.outlet.OnEvent(event)
						}
						return consumer.ConsumeSuccess, nil
					})
				if err != nil {
					panic(err)
				}
			}
		}()
	})
}
func (input *rocketmqInput) checkTags(msg *primitive.MessageExt) bool {
	if input.config.RouteTag == "" {
		return true
	}
	return input.config.RouteTag == msg.GetTags()
}
func (input *rocketmqInput) createEvents(msgs ...*primitive.MessageExt) []beat.Event {
	timestamp := time.Now()
	var events []beat.Event
	for _, msg := range msgs {
		if !input.checkTags(msg) {
			continue
		}
		event := beat.Event{
			Timestamp: timestamp,
			Fields: common.MapStr{
				"message": string(msg.Body),
				"rocketmq": common.MapStr{
					"topic":         msg.Topic,
					"msg_id":        msg.MsgId,
					"tid":           msg.TransactionId,
					"offset_msg_id": msg.OffsetMsgId,
					"broker_name":   msg.Queue.BrokerName,
				},
			},
		}
		events = append(events, event)
	}
	return events
}
func (input *rocketmqInput) Stop() {
	input.consumer.Shutdown()
}
func (input *rocketmqInput) Wait() {
	input.consumer.Unsubscribe(input.rocketmqConfig.topic)
	input.Stop()
}

type channelCtx <-chan struct{}

func doneChannelContext(ch <-chan struct{}) context.Context {
	return channelCtx(ch)
}

func (c channelCtx) Deadline() (deadline time.Time, ok bool) { return }
func (c channelCtx) Done() <-chan struct{} {
	return (<-chan struct{})(c)
}
func (c channelCtx) Err() error {
	select {
	case <-c:
		return context.Canceled
	default:
		return nil
	}
}
func (c channelCtx) Value(key interface{}) interface{} { return nil }
