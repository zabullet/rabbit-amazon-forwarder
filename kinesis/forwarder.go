package kinesis

import (
	"encoding/json"
	"errors"
	"math/rand"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/AirHelp/rabbit-amazon-forwarder/config"
	"github.com/AirHelp/rabbit-amazon-forwarder/forwarder"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

const (
	// Type forwarder type
	Type = "Kinesis"
)

// see https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
const maxQUEUELENGTH = 500

// Config is the json representation of a kenisis config
type Config struct {
	Configversion            *float64 `json:"configversion"`
	StreamName               string   `json:"stream"`
	MaxQueueBufferTimeMillis uint16   `json:"maxqueuebuffertimemillis"`
}

// Forwarder forwarding client
type Forwarder struct {
	name            string
	kinesisClient   kinesisiface.KinesisAPI
	outputQ         *[]*kinesis.PutRecordsRequestEntry
	lastSuccessTime *int64
	config          Config
}

// CreateForwarder creates instance of forwarder
func CreateForwarder(entry config.Entry, kinesisClient ...kinesisiface.KinesisAPI) forwarder.Client {

	//Unmarshal Config
	if entry.Config == nil {
		//we need a config
		return nil
	}

	var config Config
	if err := json.Unmarshal(*entry.Config, &config); err != nil {
		return nil
	}

	//Maintain backwards compatibility (assume V0 config)
	if (config.Configversion == nil) || (*config.Configversion != 1) {
		log.Warn("Looks like you're using an old config format version or have forgotten the configversion parameter. We will try and recover")
	}

	if config.StreamName == "" {
		log.Error("Stream name not defined. We will not start up")
		return nil
	}

	var client kinesisiface.KinesisAPI
	if len(kinesisClient) > 0 {
		client = kinesisClient[0]
	} else {
		client = kinesis.New(session.Must(session.NewSession()))
	}

	outputQ := make([]*kinesis.PutRecordsRequestEntry, 0)
	currentUnixTime := time.Now().UnixNano()
	maxQueueBufferTimeMillis := config.MaxQueueBufferTimeMillis
	if maxQueueBufferTimeMillis == 0 {
		maxQueueBufferTimeMillis = 1000
	}

	forwarder := Forwarder{entry.Name, client, &outputQ, &currentUnixTime, config}
	log.WithFields(log.Fields{"forwarderName": forwarder.Name(), "forwarderType": Type}).Info("Created forwarder")

	return forwarder
}

// Name forwarder name
func (f Forwarder) Name() string {
	return f.name
}

func (f Forwarder) flushQueuedMessages() error {

	if len(*f.outputQ) > 0 {

		log.Info("Writing out ", len(*f.outputQ), " records to Kinesis")

		inputRecords := &kinesis.PutRecordsInput{
			StreamName: aws.String(f.config.StreamName),
			Records:    *f.outputQ}

		resp, err := f.kinesisClient.PutRecords(inputRecords)

		//Create a slice to put failed messages in
		FailureQ := make([]*kinesis.PutRecordsRequestEntry, 0)
		if *resp.FailedRecordCount > 0 {
			recordCount := 0
			log.WithFields(log.Fields{
				"FailedRecordCount": *resp.FailedRecordCount}).Error("Error putting records")
			for _, item := range resp.Records {
				if item.ErrorCode != nil {
					FailureQ = append(FailureQ, (*f.outputQ)[recordCount])
				}
				recordCount++
			}
		}

		//Reset the output queue
		*f.outputQ = (*f.outputQ)[:0]

		//re-queue failed messages
		for _, failedItem := range FailureQ {
			*f.outputQ = append(*f.outputQ, failedItem)
		}

		*f.lastSuccessTime = time.Now().UnixNano()

		if err != nil {
			log.WithFields(log.Fields{
				"forwarderName": f.Name(),
				"error":         err.Error()}).Error("Could not forward message")
			return err
		}

	}

	return nil
}

// Push pushes message to forwarding infrastructure
func (f Forwarder) Push(message string) error {
	if message == "" {
		return errors.New(forwarder.EmptyMessageError)
	}

	inputRecord := &kinesis.PutRecordsRequestEntry{
		Data:         []byte(message),                            // Required
		PartitionKey: aws.String(strconv.Itoa(rand.Intn(10000)))} //use something random for partition

	*f.outputQ = append(*f.outputQ, inputRecord)

	currentUnixTime := time.Now().UnixNano()

	if (currentUnixTime-*f.lastSuccessTime >= int64(f.config.MaxQueueBufferTimeMillis)*int64(time.Millisecond)) || // Don't queue for more than maxQueueBufferTimeMillis
		(len(*f.outputQ) >= maxQUEUELENGTH) { //See notes for Kinesis PutRecords
		f.flushQueuedMessages()
	}

	return nil
}

// Stop stops the forwarder in this case it attempts a flush
func (f Forwarder) Stop() error {
	log.WithFields(log.Fields{"ForwarderName": f.Name()}).Info("Stopping Forwarder")
	f.flushQueuedMessages()
	return nil
}
