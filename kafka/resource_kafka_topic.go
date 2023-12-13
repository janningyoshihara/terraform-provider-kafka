package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/retry"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func kafkaTopicResource() *schema.Resource {
	return &schema.Resource{
		CreateContext: topicCreate,
		ReadContext:   topicRead,
		UpdateContext: topicUpdate,
		DeleteContext: topicDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		CustomizeDiff: customDiff,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the topic.",
			},
			"partitions": {
				Type:         schema.TypeInt,
				Required:     true,
				Description:  "Number of partitions.",
				ValidateFunc: validation.IntAtLeast(1),
			},
			"replication_factor": {
				Type:         schema.TypeInt,
				Required:     true,
				ForceNew:     false,
				Description:  "Number of replicas.",
				ValidateFunc: validation.IntAtLeast(1),
			},
			"config": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    false,
				Description: "A map of string k/v attributes.",
				Elem:        schema.TypeString,
			},
			"bootstrap_server": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "bootstrap server of the cluster",
			},
		},
	}
}

func topicCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {

	//send lambda response to validateTopicCreation
	c := meta.(*lambda.Lambda)
	t := metaToTopic(d, meta)

	// Create a map to represent the JSON structure
	payload := map[string]interface{}{
		"queryStringParameters": map[string]string{
			"type_query": "listTopics",
		},
	}

	// Convert the map to JSON
	payload_json, err := json.Marshal(payload)
	if err != nil {
		fmt.Println("Error:", err)
		return diag.FromErr(fmt.Errorf("error converting payload to json (%s): %s", t.Name, err))
	}

	// Input settings for Lambda function invocation
	functionName := "test-lambda-mks"
	input := &lambda.InvokeInput{
		FunctionName:   aws.String(functionName),
		InvocationType: aws.String(lambda.InvocationTypeRequestResponse), // JSON encoded value in Payload Set 	Payload: b, }
		Payload:        payload_json,
	}
	// Lambda function call
	resp, err := c.InvokeWithContext(ctx, input)
	if err != nil {
		return diag.FromErr(fmt.Errorf("error invoking lambda, function name = %s: %w", functionName, err))
	}

	stateConf := &retry.StateChangeConf{
		Pending:      []string{"Pending"},
		Target:       []string{"Created"},
		Refresh:      validateTopicCreation(resp, t),
		Timeout:      30 * time.Second,
		Delay:        1 * time.Second,
		PollInterval: 2 * time.Second,
	}

	if _, err := stateConf.WaitForStateContext(ctx); err != nil {
		return diag.FromErr(fmt.Errorf("error creating topic (%s): %s", t.Name, err))
	}

	d.SetId(t.Name)
	return nil
}

func validateTopicCreation(response *lambda.InvokeOutput, t Topic) retry.StateRefreshFunc {

	var response_iter map[string]interface{}
	if err := json.Unmarshal(response.Payload, &response_iter); err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("lambda response payload %+v\n", response_iter)

	// check whether topic has been created (parse lambda response from topicCreate)
	return func() (result interface{}, state string, err error) {
		switch response_iter["statusCode"] {
		case 200:
			return response_iter["body"], "Created", nil
		default:
			return response_iter["body"], "Error", errors.New("lambda invoke topic creation failed")
		}
	}
}

func topicUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {

	//TODO: call lambda with updateTopic

	// c := meta.(*LazyClient)
	// t := metaToTopic(d, meta)

	// if err := c.UpdateTopic(t); err != nil {
	// 	return diag.FromErr(err)
	// }

	// // update replica count of existing partitions before adding new ones
	// if d.HasChange("replication_factor") {
	// 	oi, ni := d.GetChange("replication_factor")
	// 	oldRF := oi.(int)
	// 	newRF := ni.(int)
	// 	log.Printf("[INFO] Updating replication_factor from %d to %d", oldRF, newRF)
	// 	t.ReplicationFactor = int16(newRF)

	// 	if err := c.AlterReplicationFactor(t); err != nil {
	// 		return diag.FromErr(err)
	// 	}

	// 	if err := waitForRFUpdate(ctx, c, d.Id()); err != nil {
	// 		return diag.FromErr(err)
	// 	}
	// }

	// if d.HasChange("partitions") {
	// 	// update should only be called when we're increasing partitions
	// 	oi, ni := d.GetChange("partitions")
	// 	oldPartitions := oi.(int)
	// 	newPartitions := ni.(int)
	// 	log.Printf("[INFO] Updating partitions from %d to %d", oldPartitions, newPartitions)
	// 	t.Partitions = int32(newPartitions)

	// 	if err := c.AddPartitions(t); err != nil {
	// 		return diag.FromErr(err)
	// 	}
	// }

	// if err := waitForTopicRefresh(ctx, c, d.Id(), t); err != nil {
	// 	return diag.FromErr(err)
	// }

	return nil
}

// 	}
// }

func topicDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	// c := meta.(*LazyClient)
	// t := metaToTopic(d, meta)

	// err := c.DeleteTopic(t.Name)
	// if err != nil {
	// 	return diag.FromErr(err)
	// }

	// log.Printf("[DEBUG] waiting for topic to delete? %s", t.Name)
	// stateConf := &retry.StateChangeConf{
	// 	Pending:      []string{"Pending"},
	// 	Target:       []string{"Deleted"},
	// 	Refresh:      topicDeleteFunc(c, d.Id(), t),
	// 	Timeout:      300 * time.Second,
	// 	Delay:        3 * time.Second,
	// 	PollInterval: 2 * time.Second,
	// 	MinTimeout:   20 * time.Second,
	// }
	// _, err = stateConf.WaitForStateContext(ctx)
	// if err != nil {
	// 	return diag.FromErr(fmt.Errorf("Error waiting for topic (%s) to delete: %s", d.Id(), err))
	// }

	// log.Printf("[DEBUG] deletetopic done! %s", t.Name)
	// d.SetId("")
	return nil
}

// func topicDeleteFunc(client *LazyClient, id string, t Topic) retry.StateRefreshFunc {
// 	return func() (result interface{}, s string, err error) {
// 		topic, err := client.ReadTopic(t.Name, true)

// 		log.Printf("[DEBUG] deletetopic read %s, %v", t.Name, err)
// 		if err != nil {
// 			_, ok := err.(TopicMissingError)
// 			if ok {
// 				return topic, "Deleted", nil
// 			}
// 			return topic, "UNKNOWN", err
// 		}
// 		return topic, "Pending", nil
// 	}
// }

func topicRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	// name := d.Id() //gets topic name
	// client := meta.(*lambda.Lambda)
	// topic, err := client.ReadTopic(name, false) //call lambda to retrieve topic

	// if err != nil {
	// 	log.Printf("[ERROR] Error getting topics %s from Kafka", err)
	// 	_, ok := err.(TopicMissingError)
	// 	if ok {
	// 		d.SetId("")
	// 		return nil
	// 	}

	// 	return diag.FromErr(err)
	// }

	// log.Printf("[DEBUG] Setting the state from Kafka %v", topic)
	// errSet := errSetter{d: d}
	// errSet.Set("name", topic.Name)
	// errSet.Set("partitions", topic.Partitions)
	// errSet.Set("replication_factor", topic.ReplicationFactor)
	// errSet.Set("config", topic.Config)

	// if errSet.err != nil {
	// 	return diag.FromErr(errSet.err)
	// }

	return nil
}

func customDiff(ctx context.Context, diff *schema.ResourceDiff, v interface{}) error {
	// Skip custom logic for resource creation.
	// if diff.Id() == "" {
	// 	return nil
	// }
	// if diff.HasChange("partitions") {
	// 	log.Printf("[INFO] Partitions have changed!")
	// 	o, n := diff.GetChange("partitions")
	// 	oi := o.(int)
	// 	ni := n.(int)
	// 	log.Printf("[INFO] Partitions is changing from %d to %d", oi, ni)
	// 	if ni < oi {
	// 		log.Printf("Partitions decreased from %d to %d. Forcing new resource", oi, ni)
	// 		if err := diff.ForceNew("partitions"); err != nil {
	// 			return err
	// 		}
	// 	}
	// }

	// if diff.HasChange("replication_factor") {
	// 	log.Printf("[INFO] Checking the diff!")
	// 	client := v.(*LazyClient)

	// 	canAlterRF, err := client.CanAlterReplicationFactor()
	// 	if err != nil {
	// 		return err
	// 	}

	// 	if !canAlterRF {
	// 		log.Println("[INFO] Need Kafka >= 2.4.0 to update replication_factor in-place")
	// 		if err := diff.ForceNew("replication_factor"); err != nil {
	// 			return err
	// 		}
	// 	}
	// }

	return nil
}
