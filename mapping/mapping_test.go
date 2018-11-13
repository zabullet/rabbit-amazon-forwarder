package mapping

import (
	"errors"
	"os"
	"testing"

	"github.com/AirHelp/rabbit-amazon-forwarder/config"
	"github.com/AirHelp/rabbit-amazon-forwarder/consumer"
	"github.com/AirHelp/rabbit-amazon-forwarder/forwarder"
	"github.com/AirHelp/rabbit-amazon-forwarder/lambda"
	"github.com/AirHelp/rabbit-amazon-forwarder/rabbitmq"
	"github.com/AirHelp/rabbit-amazon-forwarder/sns"
	"github.com/AirHelp/rabbit-amazon-forwarder/sqs"
)

const (
	rabbitType = "rabbit"
	snsType    = "sns"
)

func TestLoadV0(t *testing.T) {
	os.Setenv(config.MappingFile, "../tests/rabbit_to_sns.json")
	client := New(MockMappingHelper{})
	var consumerForwarderMap map[consumer.Client]forwarder.Client
	var err error
	if consumerForwarderMap, err = client.Load(); err != nil {
		t.Errorf("could not load mapping and start mocked rabbit->sns pair: %s", err.Error())
	}
	if len(consumerForwarderMap) != 1 {
		t.Errorf("wrong consumerForwarderMap size, expected 1, got %d", len(consumerForwarderMap))
	}
}

func TestLoadV2(t *testing.T) {
	os.Setenv(config.MappingFile, "../tests/rabbit_to_sns.v2.json")
	client := New(MockMappingHelper{})
	var consumerForwarderMap map[consumer.Client]forwarder.Client
	var err error
	if consumerForwarderMap, err = client.Load(); err != nil {
		t.Errorf("could not load mapping and start mocked rabbit->sns pair: %s", err.Error())
	}
	if len(consumerForwarderMap) != 1 {
		t.Errorf("wrong consumerForwarderMap size, expected 1, got %d", len(consumerForwarderMap))
	}
}

func TestLoadFile(t *testing.T) {
	os.Setenv(config.MappingFile, "../tests/rabbit_to_sns.json")
	client := New()
	data, err := client.loadFile()
	if err != nil {
		t.Errorf("could not load file: %s", err.Error())
	}
	if len(data) < 1 {
		t.Errorf("could not load file: empty steam found")
	}
}

// func TestCreateConsumerV1Config(t *testing.T) {
// 	client := New(MockMappingHelper{})
// 	consumerName := "test-rabbit"

// 	entry := config.Entry{
// 		Type: "RabbitMQ",
// 		Name: consumerName,
// 	}
// 	consumer := client.helper.createConsumer(entry)
// 	if consumer.Name() != consumerName {
// 		t.Errorf("wrong consumer name, expected %s, found %s", consumerName, consumer.Name())
// 	}
// }

// func TestCreateForwarderSNS(t *testing.T) {
// 	client := New(MockMappingHelper{})
// 	forwarderName := "test-sns"

// 	rawConfig, _ := json.Marshal(sns.Config{
// 		Topic: "topic1",
// 	})

// 	entry := config.Entry{
// 		Type:   "SNS",
// 		Name:   forwarderName,
// 		Config: (*json.RawMessage)(&rawConfig),
// 	}

// 	forwarder := client.helper.createForwarder(entry)
// 	if forwarder.Name() != forwarderName {
// 		t.Errorf("wrong forwarder name, expected %s, found %s", forwarderName, forwarder.Name())
// 	}
// }

// func TestCreateForwarderSQS(t *testing.T) {
// 	client := New(MockMappingHelper{})
// 	forwarderName := "test-sqs"
// 	rawConfig, _ := json.Marshal(sqs.Config{
// 		Queue: "arn",
// 	})

// 	entry := config.Entry{
// 		Type:   "SQS",
// 		Name:   forwarderName,
// 		Config: (*json.RawMessage)(&rawConfig),
// 	}
// 	forwarder := client.helper.createForwarder(entry)
// 	if forwarder.Name() != forwarderName {
// 		t.Errorf("wrong forwarder name, expected %s, found %s", forwarderName, forwarder.Name())
// 	}
// }

// func TestCreateForwarderLambdaConfigV2(t *testing.T) {
// 	client := New(MockMappingHelper{})
// 	forwarderName := "test-lambda"

// 	rawConfig, _ := json.Marshal(lambda.ConfigV2{
// 		Configversion: aws.String("v2"),
// 		Function:      "function-name",
// 	})

// 	entry := config.Entry{
// 		Type:   "Lambda",
// 		Name:   forwarderName,
// 		Config: (*json.RawMessage)(&rawConfig),
// 	}
// 	forwarder := client.helper.createForwarder(entry)
// 	if forwarder.Name() != forwarderName {
// 		t.Errorf("wrong forwarder name, expected %s, found %s", forwarderName, forwarder.Name())
// 	}
// }

// helpers
type MockMappingHelper struct{}

type MockRabbitConsumer struct{}

type MockSNSForwarder struct {
	name string
}

type MockSQSForwarder struct {
	name string
}

type MockLambdaForwarder struct {
	name string
}

type ErrorForwarder struct{}

func (h MockMappingHelper) createConsumer(entry config.Entry) consumer.Client {
	if entry.Type != rabbitmq.Type {
		return nil
	}
	return MockRabbitConsumer{}
}
func (h MockMappingHelper) createForwarder(entry config.Entry) forwarder.Client {
	switch entry.Type {
	case sns.Type:
		return MockSNSForwarder{entry.Name}
	case sqs.Type:
		return MockSQSForwarder{entry.Name}
	case lambda.Type:
		return MockLambdaForwarder{entry.Name}
	}
	return ErrorForwarder{}
}

func (c MockRabbitConsumer) Name() string {
	return rabbitType
}

func (c MockRabbitConsumer) Start(client forwarder.Client, check chan bool, stop chan bool) error {
	return nil
}

func (c MockRabbitConsumer) Stop() error {
	return nil
}

func (f MockSNSForwarder) Name() string {
	return f.name
}

func (f MockSNSForwarder) Push(message string) error {
	return nil
}

func (f MockSNSForwarder) Stop() error {
	return nil
}

func (f MockSQSForwarder) Name() string {
	return f.name
}

func (f MockLambdaForwarder) Push(message string) error {
	return nil
}

func (f MockLambdaForwarder) Stop() error {
	return nil
}

func (f MockLambdaForwarder) Name() string {
	return f.name
}

func (f MockSQSForwarder) Push(message string) error {
	return nil
}

func (f MockSQSForwarder) Stop() error {
	return nil
}

func (f ErrorForwarder) Name() string {
	return "error-forwarder"
}

func (f ErrorForwarder) Push(message string) error {
	return errors.New("Wrong forwader created")
}

func (f ErrorForwarder) Stop() error {
	return nil
}
