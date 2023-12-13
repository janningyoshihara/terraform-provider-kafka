package kafka

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type Topic struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Config            map[string]*string
}

func (t *Topic) Equal(other Topic) bool {
	mape := MapEq(other.Config, t.Config)

	if mape == nil && (other.Name == t.Name) && (other.Partitions == t.Partitions) && (other.ReplicationFactor == t.ReplicationFactor) {
		return true
	}
	return false
}

func metaToTopic(d *schema.ResourceData, meta interface{}) Topic {
	topicName := d.Get("name").(string)
	partitions := d.Get("partitions").(int)
	replicationFactor := d.Get("replication_factor").(int)
	convertedPartitions := int32(partitions)
	convertedRF := int16(replicationFactor)
	config := d.Get("config").(map[string]interface{})

	m2 := make(map[string]*string)
	for key, value := range config {
		switch value := value.(type) {
		case string:
			m2[key] = &value
		}
	}

	return Topic{
		Name:              topicName,
		Partitions:        convertedPartitions,
		ReplicationFactor: convertedRF,
		Config:            m2,
	}
}
