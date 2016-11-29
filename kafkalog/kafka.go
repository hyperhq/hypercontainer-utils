package kafkalog

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/docker/docker/daemon/logger"
	"github.com/golang/glog"
)

const Name = "kafkalog"

var kafkaConsumerCli sarama.Client = nil
var consumerLock sync.Mutex
var conn_number = 10
var kafkaProducers = make([]*kafkaProducer, conn_number)

func init() {
	if err := logger.RegisterLogDriver(Name, New); err != nil {
		glog.Errorf("fail to register kafkalog driver: %v", err)
	}
	if err := logger.RegisterLogOptValidator(Name, ValidateLogOpt); err != nil {
		glog.Errorf("fail to validate kafkalog driver: %v", err)
	}
}

// ValidateLogOpt looks for json specific log options max-file & max-size.
func ValidateLogOpt(cfg map[string]string) error {
	for key := range cfg {
		switch key {
		case "KafkaHosts":
		case "QueueLen":
		case "Duration":
		default:
			return fmt.Errorf("unknown log opt '%s' for kafka log driver", key)
		}
	}
	return nil
}

func initConsumerClient(hosts []string) error {
	if kafkaConsumerCli != nil && !kafkaConsumerCli.Closed() {
		return nil
	}
	// enable V 0.10.0.1 support
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_1
	var err error

	kafkaConsumerCli, err = sarama.NewClient(hosts, config)
	if err != nil {
		glog.Errorf("fail to create new kafka client: %v", err)
	}

	return err
}

func newConsumer(hosts []string, container string, cfg logger.ReadConfig) (sarama.PartitionConsumer, int64, error) {
	consumerLock.Lock()
	defer consumerLock.Unlock()

	tail := int64(cfg.Tail)

	if err := initConsumerClient(hosts); err != nil {
		glog.Errorf("Failed to initialize Sarama client: %v", err)
		return nil, 0, err
	}

	newestOffset, err := kafkaConsumerCli.GetOffset(container, 0, sarama.OffsetNewest)
	if err != nil {
		glog.Errorf("Failed to get Sarama customer newest offset: %v", err)
		return nil, 0, err
	}

	oldestOffset, err := kafkaConsumerCli.GetOffset(container, 0, sarama.OffsetOldest)
	if err != nil {
		glog.Errorf("Failed to get Sarama customer oldest offset: %v", err)
		return nil, 0, err
	}

	glog.V(3).Infof("container %v, newest %v, oldest %v", container, newestOffset, oldestOffset)
	if newestOffset == oldestOffset && !cfg.Follow {
		return nil, 0, fmt.Errorf("no logs & no follow, return immediately")
	}

	consumer, err := sarama.NewConsumerFromClient(kafkaConsumerCli)
	if err != nil {
		glog.Errorf("Failed to create Sarama customer: %v", err)
		return nil, 0, err
	}

	offset := sarama.OffsetOldest
	if tail > 0 {
		len := newestOffset - oldestOffset

		if len > tail {
			offset = newestOffset - tail
		}
	}

	glog.V(3).Infof("tail %v, offset %v", tail, offset)

	partitionConsumer, err := consumer.ConsumePartition(container, 0, offset)
	if err != nil {
		glog.Errorf("Failed to create Sarama customerPartition: %v", err)
	}

	return partitionConsumer, newestOffset, err
}

func NewAsyncProducer(hosts []string) (sarama.AsyncProducer, error) {
	// enable V 0.10.0.1 support
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_1
	config.Producer.RequiredAcks = sarama.WaitForAll

	glog.V(3).Infof("hosts %v", hosts)
	producer, err := sarama.NewAsyncProducer(hosts, config)
	if err != nil {
		glog.Errorf("fail to connect to kafka: %v", err)
		return nil, err
	}

	return producer, nil
}

type kafkaProducer struct {
	producer sarama.AsyncProducer
	logChan  chan *logger.Message
	sync.Mutex
}

func Init(hosts []string, duration, queueLen int) error {
	if queueLen == 0 {
		return fmt.Errorf("please specify the length of log channel")
	}

	if duration == 0 {
		return fmt.Errorf("please specify the duration of kafka health checker")
	}

	for i := 0; i < conn_number; i++ {
		producer, err := NewAsyncProducer(hosts)
		if err != nil {
			return fmt.Errorf("[producer]: fail to connect to kafka: %v", err)
		}

		kp := &kafkaProducer{
			producer: producer,
			logChan:  make(chan *logger.Message, queueLen),
		}

		kafkaProducers[i] = kp
		var number uint64 = 0
		go func() {
			for log := range kp.logChan {
				logline := append(log.Line, '\n')

				msg := &sarama.ProducerMessage{
					Topic:     log.ContainerID,
					Timestamp: log.Timestamp,
					Value:     &KafkaLogs{Log: logline, Stream: log.Source},
				}

				kp.Lock()
				if kp.producer == nil {
					kp.Unlock()
					continue
				}

				select {
				case err := <-kp.producer.Errors():
					glog.Errorf("FAILED to send message: %v", err)
					producer := kp.producer
					go func() {
						// Close may cost much time
						producer.Close()
					}()
					kp.producer = nil
				case kp.producer.Input() <- msg:
					number++
				}
				kp.Unlock()
			}
		}()

		go func() {
			ticker := time.NewTicker(time.Duration(duration) * time.Second)
			for _ = range ticker.C {
				glog.V(3).Infof("producer %v: length of chan: %v, %v producers %v",
					kp, len(kp.logChan), number, kp.producer)

				kp.Lock()
				if kp.producer == nil {
					kp.Unlock()

					producer, err := NewAsyncProducer(hosts)
					if err != nil {
						glog.Errorf("[producer]: fail to connect to kafka: %v", err)
						continue
					} else {
						glog.Infof("Connected to kafka")
					}

					kp.Lock()
					if kp.producer == nil {
						kp.producer = producer
					} else {
						// Can not happen
						glog.V(3).Infof("kafka producer %v alreay connected", kp)
						go func() {
							producer.Close()
						}()
					}
				}
				kp.Unlock()
			}
		}()
	}

	return nil
}

type KafkaLogs struct {
	Log     []byte
	Stream  string
	encoded []byte
	err     error
}

func (d *KafkaLogs) ensureEncoded() {
	if d.encoded == nil && d.err == nil {
		data := [][]byte{[]byte(d.Stream), d.Log}
		d.encoded = bytes.Join(data, []byte(", "))
	}
}

func (d *KafkaLogs) Length() int {
	d.ensureEncoded()
	return len(d.encoded)
}

func (d *KafkaLogs) Encode() ([]byte, error) {
	d.ensureEncoded()
	return d.encoded, d.err
}

func hash(id string) int {
	h := fnv.New32a()
	h.Write([]byte(id))
	return int(h.Sum32()) % conn_number
}

type KafkaLogger struct {
	logs      chan *logger.Message
	number    uint64
	container string
	hosts     string
	mu        sync.Mutex
	readers   map[*logger.LogWatcher]struct{} // stores the active log followers
}

func New(ctx logger.Context) (logger.Logger, error) {
	if _, ok := ctx.Config["KafkaHosts"]; !ok {
		return nil, fmt.Errorf("Cannot find kafkaHosts in configuration")
	}

	logger := &KafkaLogger{
		hosts:     ctx.Config["KafkaHosts"],
		container: ctx.ContainerID,
		readers:   make(map[*logger.LogWatcher]struct{}),
	}

	key := hash(ctx.ContainerID)
	glog.V(3).Infof("container %v use %d client", ctx.ContainerID, key)
	if kp := kafkaProducers[key]; kp != nil {
		logger.logs = kp.logChan
	}

	return logger, nil
}

func (k *KafkaLogger) Log(msg *logger.Message) error {
	k.logs <- msg
	k.number++

	return nil
}

func (k *KafkaLogger) ReadLogs(config logger.ReadConfig) *logger.LogWatcher {
	logWatcher := logger.NewLogWatcher()

	go k.readLogs(logWatcher, config)
	return logWatcher
}

func (k *KafkaLogger) readLogs(logWatcher *logger.LogWatcher, cfg logger.ReadConfig) {
	defer close(logWatcher.Msg)

	glog.V(3).Infof("readLogs cfg %v\n", cfg)

	consumer, newestOffset, err := newConsumer(strings.Split(k.hosts, ","), k.container, cfg)
	if err != nil {
		glog.Errorf("fail to create consumer: %v", err)
		logWatcher.Err <- fmt.Errorf("fail to get logs from log server")
		return
	}

	defer consumer.Close()

	k.mu.Lock()
	k.readers[logWatcher] = struct{}{}
	k.mu.Unlock()

	defer func() {
		k.mu.Lock()
		delete(k.readers, logWatcher)
		k.mu.Unlock()
	}()

	for {
		select {
		case log, ok := <-consumer.Messages():
			if !ok {
				logWatcher.Err <- fmt.Errorf("fail to get logs from log server")
				return
			}

			data := bytes.SplitN([]byte(log.Value), []byte(", "), 2)
			if len(data) != 2 {
				glog.Errorf("unmarshal failed: %v", err)
				continue
			}

			if !cfg.Since.IsZero() && log.Timestamp.Before(cfg.Since) {
				continue
			}

			logWatcher.Msg <- &logger.Message{
				Source:    string(data[0]),
				Timestamp: log.Timestamp,
				Line:      data[1],
			}

			if log.Offset == newestOffset-1 && !cfg.Follow {
				return
			}
		case <-logWatcher.WatchClose():
			return
		}
	}
}

func (k *KafkaLogger) Name() string {
	return Name
}

func (k *KafkaLogger) Close() error {
	k.mu.Lock()
	for r := range k.readers {
		r.Close()
		delete(k.readers, r)
	}
	k.mu.Unlock()

	glog.V(3).Infof("container %v already wrote %d logs", k.container, k.number)

	return nil
}
