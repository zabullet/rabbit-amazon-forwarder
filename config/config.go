package config

import (
	"encoding/json"
)

const (
	// MappingFile mapping file environment variable
	MappingFile = "MAPPING_FILE"
)

// Entry Generic config entry
type Entry struct {
	Type   string                 `json:"type"`
	Name   string                 `json:"name"`
	Config *json.RawMessage       `json:"config"`
}

// RabbitArgumentsPair describes a rabbitMQ bind or declare argument
type RabbitArgumentsPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// RabbitTopographyItem describes an element of the rabbit topography to set up
type RabbitTopographyItem struct {
	Name      string                 `json:"name"`
	Action    string                 `json:"action"`
	Type      string                 `json:"type"`
	Kind      string                 `json:"kind"`
	Routekey  string                 `json:"routekey"`
	To        string                 `json:"to"`
	Arguments *[]RabbitArgumentsPair `json:"args"`
}
