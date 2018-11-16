package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	log "github.com/sirupsen/logrus"

	"github.com/AirHelp/rabbit-amazon-forwarder/config"
	"github.com/AirHelp/rabbit-amazon-forwarder/consumer"
	"github.com/AirHelp/rabbit-amazon-forwarder/forwarder"
	"github.com/streadway/amqp"
)

const (
	// Type consumer type
	Type                      = "RabbitMQ"
	channelClosedMessage      = "Channel closed"
	closedBySupervisorMessage = "Closed by supervisor"
	// ReconnectRabbitMQInterval time to reconnect
	ReconnectRabbitMQInterval = 10
)

// RabbitArgumentsPair describes a rabbitMQ bind or declare argument
type RabbitArgumentsPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// RabbitTopologyItem describes an element of the rabbit topology to set up
type RabbitTopologyItem struct {
	Name       string                 `json:"name"`
	Action     string                 `json:"action"`
	Type       string                 `json:"type"`
	Kind       string                 `json:"kind"`
	Routekey   string                 `json:"routekey"`
	To         string                 `json:"to"`
	Durable    bool                   `json:"durable"`
	AutoDelete bool                   `json:"autodelete"`
	Exclusive  bool                   `json:"exclusive"`
	NoWait     bool                   `json:"nowait"`
	Internal   bool                   `json:"internal"`
	Arguments  *[]RabbitArgumentsPair `json:"args"`
}

// Config RabbitMQ config entry
type Config struct {
	ConnectionURL  string                `json:"connection"`
	RabbitTopology *[]RabbitTopologyItem `json:"rabbittopology"`
	WrapMessage    bool                  `json:"wrapmessage"` //specifies whether the rabbit message should be wrapped in a json struct
	Configversion  *float64              `json:"configversion"`
	QueueName      string                `json:"queue"`
}

// Consumer implementation or RabbitMQ consumer
type Consumer struct {
	name   string
	config Config
}

// parameters for starting consumer
type workerParams struct {
	forwarder forwarder.Client
	msgs      <-chan amqp.Delivery
	check     chan bool
	stop      chan bool
	conn      *amqp.Connection
	ch        *amqp.Channel
}

// CreateConsumer creates consumer from string map
func CreateConsumer(entry config.Entry) consumer.Client {
	if entry.Config == nil {
		//we need a config
		return nil
	}

	var config Config
	if err := json.Unmarshal(*entry.Config, &config); err != nil {
		return nil
	}

	if config.Configversion == nil {
		log.Warn("Looks like you're using an old config format version or have forgotten the configversion parameter. We will try and recover")
	}

	return Consumer{entry.Name, config}
}

// Name consumer name
func (c Consumer) Name() string {
	return c.name
}

// Start start consuming messages from Rabbit queue
func (c Consumer) Start(forwarder forwarder.Client, check chan bool, stop chan bool) error {
	log.WithFields(log.Fields{
		"queueName": c.config.QueueName}).Info("Starting connecting consumer")
	for {
		delivery, conn, ch, err := c.initRabbitMQ()
		if err != nil {
			log.Error(err)
			closeRabbitMQ(conn, ch)
			time.Sleep(ReconnectRabbitMQInterval * time.Second)
			continue
		}
		params := workerParams{forwarder, delivery, check, stop, conn, ch}
		if err := c.startForwarding(&params); err.Error() == closedBySupervisorMessage {
			break
		}
	}
	return nil
}

func closeRabbitMQ(conn *amqp.Connection, ch *amqp.Channel) {
	log.Info("Closing RabbitMQ connection and channel")
	if ch != nil {
		if err := ch.Close(); err != nil {
			log.WithField("error", err.Error()).Error("Could not close channel")
		}
	}
	if conn != nil {
		if err := conn.Close(); err != nil {
			log.WithField("error", err.Error()).Error("Could not close connection")
		}
	}
}

func (c Consumer) initRabbitMQ() (<-chan amqp.Delivery, *amqp.Connection, *amqp.Channel, error) {
	_, connection, channel, err := c.connect()
	if err != nil {
		return nil, connection, channel, err
	}
	delivery, _, _, err := c.setupExchangesAndQueues(connection, channel)
	return delivery, connection, channel, err
}

func (c Consumer) connect() (<-chan amqp.Delivery, *amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(c.config.ConnectionURL)
	if err != nil {
		return failOnError(err, "Failed to connect to RabbitMQ")
	}
	ch, err := conn.Channel()
	if err != nil {
		return failOnError(err, "Failed to open a channel")
	}
	return nil, conn, ch, nil
}

func (c Consumer) setupExchangesAndQueues(conn *amqp.Connection, ch *amqp.Channel) (<-chan amqp.Delivery, *amqp.Connection, *amqp.Channel, error) {
	// Setup the topology
	if c.config.RabbitTopology != nil {
		for _, topologyItem := range *c.config.RabbitTopology {
			amqpArg := amqp.Table{}
			if topologyItem.Arguments != nil {
				for _, topologyArgument := range *topologyItem.Arguments {
					amqpArg[topologyArgument.Key] = topologyArgument.Value
				}
			}

			switch topologyItem.Action {
			case "declare":
				{
					switch topologyItem.Type {
					case "exchange":
						{
							ch.ExchangeDeclare(
								topologyItem.Name,
								topologyItem.Kind,
								topologyItem.Durable,
								topologyItem.AutoDelete,
								topologyItem.Internal,
								topologyItem.NoWait,
								amqpArg)
						}
					case "queue":
						{
							ch.QueueDeclare(
								topologyItem.Name,
								topologyItem.Durable,
								topologyItem.AutoDelete,
								topologyItem.Exclusive,
								topologyItem.NoWait,
								amqpArg)
						}
					}
				}
			case "bind":
				{
					switch topologyItem.Type {
					case "queue":
						{
							ch.QueueBind(topologyItem.Name, topologyItem.Routekey, topologyItem.To, topologyItem.NoWait, amqpArg)
						}
					}
				}
			}
		}
	}

	// var err error
	// deadLetterExchangeName := c.config.QueueName + "-dead-letter"
	// deadLetterQueueName := c.config.QueueName + "-dead-letter"
	// // regular exchange
	// if err = ch.ExchangeDeclare(c.config.ExchangeName, "topic", true, false, false, false, nil); err != nil {
	// 	return failOnError(err, "Failed to declare an exchange:"+c.config.ExchangeName)
	// }
	// // dead-letter-exchange
	// log.WithFields(log.Fields{
	// 	"ExchangeName": deadLetterExchangeName}).Info("Declaring fanout Exchange")
	// if err = ch.ExchangeDeclare(deadLetterExchangeName, "fanout", true, false, false, false, nil); err != nil {
	// 	return failOnError(err, "Failed to declare an exchange:"+deadLetterExchangeName)
	// }
	// // dead-letter-queue
	// log.WithFields(log.Fields{
	// 	"QueueName": deadLetterQueueName}).Info("Declaring Queue")
	// if _, err = ch.QueueDeclare(deadLetterQueueName, true, false, false, false, nil); err != nil {
	// 	return failOnError(err, "Failed to declare a queue:"+deadLetterQueueName)
	// }
	// log.WithFields(log.Fields{
	// 	"QueueName":    deadLetterQueueName,
	// 	"ExchangeName": deadLetterExchangeName}).Info("Binding Queue")
	// if err = ch.QueueBind(deadLetterQueueName, "#", deadLetterExchangeName, false, nil); err != nil {
	// 	return failOnError(err, "Failed to bind a queue:"+deadLetterQueueName)
	// }
	// // regular queue
	// if _, err = ch.QueueDeclare(c.config.QueueName, true, false, false, false,
	// 	amqp.Table{
	// 		"x-dead-letter-exchange": deadLetterExchangeName,
	// 	}); err != nil {
	// 	return failOnError(err, "Failed to declare a queue:"+c.config.QueueName)
	// }
	// if err = ch.QueueBind(c.config.QueueName, c.config.RoutingKey, c.config.ExchangeName, false, nil); err != nil {
	// 	return failOnError(err, "Failed to bind a queue:"+c.config.QueueName)
	// }

	msgs, err := ch.Consume(c.config.QueueName, c.Name(), false, false, false, false, nil)
	if err != nil {
		return failOnError(err, "Failed to register a consumer")
	}
	return msgs, nil, nil, nil
}

func (c Consumer) startForwarding(params *workerParams) error {
	forwarderName := params.forwarder.Name()
	log.WithFields(log.Fields{
		"consumerName":  c.Name(),
		"forwarderName": forwarderName}).Info("Started forwarding messages")
	for {
		select {
		case d, ok := <-params.msgs:
			if !ok { // channel already closed
				closeRabbitMQ(params.conn, params.ch)
				return errors.New(channelClosedMessage)
			}
			log.WithFields(log.Fields{
				"consumerName": c.Name(),
				"messageID":    d.MessageId}).Debug("Message to forward")

			var message []byte

			if c.config.WrapMessage == true {
				mm := map[string]interface{}{
					"Headers":     "",
					"Payload":     "",
					"ContentType": "",
					"Exchange":    "",
				}

				var unMarshalledBody map[string]interface{}
				unmarshalErr := json.Unmarshal(d.Body, &unMarshalledBody)

				//we didn't get
				if unmarshalErr != nil {
					if utf8.Valid(d.Body) {
						mm["Payload"] = string(d.Body)
					} else {
						mm["Payload"] = d.Body //base64 encode the data!
					}
				} else {
					mm["Payload"] = unMarshalledBody
				}

				mm["Headers"] = d.Headers
				mm["ContentType"] = d.ContentType
				mm["Exchange"] = d.Exchange

				message, _ = json.Marshal(mm)

			} else {
				message = d.Body
			}

			err := params.forwarder.Push(string(message))

			if err != nil {
				log.WithFields(log.Fields{
					"forwarderName": forwarderName,
					"error":         err.Error()}).Error("Could not forward message")
				if err = d.Reject(false); err != nil {
					log.WithFields(log.Fields{
						"forwarderName": forwarderName,
						"error":         err.Error()}).Error("Could not reject message")
					return err
				}

			} else {
				if err := d.Ack(true); err != nil {
					log.WithFields(log.Fields{
						"forwarderName": forwarderName,
						"error":         err.Error(),
						"messageID":     d.MessageId}).Error("Could not ack message")
					return err
				}
			}
		case <-params.check:
			log.WithField("forwarderName", forwarderName).Info("Checking")
		case <-params.stop:
			log.WithField("forwarderName", forwarderName).Info("Closing")
			closeRabbitMQ(params.conn, params.ch)
			params.forwarder.Stop()
			return errors.New(closedBySupervisorMessage)
		}
	}
}

func failOnError(err error, msg string) (<-chan amqp.Delivery, *amqp.Connection, *amqp.Channel, error) {
	return nil, nil, nil, fmt.Errorf("%s: %s", msg, err)
}
