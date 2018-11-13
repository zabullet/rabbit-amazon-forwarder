package mapping

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/AirHelp/rabbit-amazon-forwarder/config"
	"github.com/AirHelp/rabbit-amazon-forwarder/consumer"
	"github.com/AirHelp/rabbit-amazon-forwarder/forwarder"
	"github.com/AirHelp/rabbit-amazon-forwarder/kinesis"
	"github.com/AirHelp/rabbit-amazon-forwarder/lambda"
	"github.com/AirHelp/rabbit-amazon-forwarder/rabbitmq"
	"github.com/AirHelp/rabbit-amazon-forwarder/sns"
	"github.com/AirHelp/rabbit-amazon-forwarder/sqs"
)

type pairs []pair

type pair struct {
	Source      *config.Entry `json:"source"`
	Destination *config.Entry `json:"destination"`
}

//Backward compatibility config
type pairsV0 []pairV0

type pairV0 struct {
	Source      *json.RawMessage `json:"source"`
	Destination *json.RawMessage `json:"destination"`
}

// Client mapping client
type Client struct {
	helper Helper
}

// Helper interface for creating consumers and forwaders
type Helper interface {
	createConsumer(entry config.Entry) consumer.Client
	createForwarder(entry config.Entry) forwarder.Client
}

type helperImpl struct{}

// New creates new mapping client
func New(helpers ...Helper) Client {
	var helper Helper
	helper = helperImpl{}
	if len(helpers) > 0 {
		helper = helpers[0]
	}
	return Client{helper}
}

// Load loads mappings
func (c Client) Load() (map[consumer.Client]forwarder.Client, error) {
	consumerForwarderMap := make(map[consumer.Client]forwarder.Client)
	data, err := c.loadFile()
	if err != nil {
		return consumerForwarderMap, err
	}

	var pairsList pairs
	if err = json.Unmarshal(data, &pairsList); err != nil {
		return consumerForwarderMap, err
	}

	//Unmarshall possible older structure
	var pairsV0List pairsV0
	if err = json.Unmarshal(data, &pairsV0List); err != nil {
		return consumerForwarderMap, err
	}

	pairsListIndex := 0

	log.Info("Loading consumer - forwarder pairs")
	for _, pair := range pairsList {

		if pair.Source == nil {
			return nil, errors.New("Source must be provided in a config pair")
		}
		if pair.Destination == nil {
			return nil, errors.New("Destination must be provided in a config pair")
		}

		//Config for source is missing?? Assume older config and push down to provider
		if pair.Source.Config == nil {
			log.Warn("Looks like you're using a V0 Config. Please consider moving to new config format")
			pair.Source.Config = pairsV0List[pairsListIndex].Source
		}

		consumer := c.helper.createConsumer(*pair.Source)
		if consumer == nil {
			return nil, fmt.Errorf("Failed to create forwarder %s type %s ", pair.Source.Name, pair.Source.Type)
		}

		//Config for destination is missing?? Assume older config and push down to provider
		if pair.Destination.Config == nil {
			log.Warn("Looks like you're using a V0 Config. Please consider moving to new config format")
			pair.Destination.Config = pairsV0List[pairsListIndex].Destination
		}

		forwarder := c.helper.createForwarder(*pair.Destination)
		if forwarder == nil {
			return nil, fmt.Errorf("Failed to create forwarder %s type %s ", pair.Destination.Name, pair.Destination.Type)
		}

		consumerForwarderMap[consumer] = forwarder
		pairsListIndex++
	}
	return consumerForwarderMap, nil
}

func (c Client) loadFile() ([]byte, error) {
	filePath := os.Getenv(config.MappingFile)
	log.WithField("mappingFile", filePath).Info("Loading mapping file")
	return ioutil.ReadFile(filePath)
}

func (h helperImpl) createConsumer(entry config.Entry) consumer.Client {
	log.WithFields(log.Fields{
		"consumerType": entry.Type,
		"consumerName": entry.Name}).Info("Creating consumer")
	switch entry.Type {
	case rabbitmq.Type:
		return rabbitmq.CreateConsumer(entry)
	}
	return nil
}

func (h helperImpl) createForwarder(entry config.Entry) forwarder.Client {
	log.WithFields(log.Fields{
		"forwarderType": entry.Type,
		"forwarderName": entry.Name}).Info("Creating forwarder")
	switch entry.Type {
	case sns.Type:
		return sns.CreateForwarder(entry)
	case sqs.Type:
		return sqs.CreateForwarder(entry)
	case lambda.Type:
		return lambda.CreateForwarder(entry)
	case kinesis.Type:
		return kinesis.CreateForwarder(entry)
	}
	return nil
}
