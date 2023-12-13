package kafka

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"region": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "us-east-1",
			},
		},

		ResourcesMap: map[string]*schema.Resource{
			"kafka_topic": kafkaTopicResource(),
		},
		DataSourcesMap: map[string]*schema.Resource{
			"kafka_topic": kafkaTopicDataSource(),
		},
		ConfigureContextFunc: configureLambdaClient,
	}
}

func configureLambdaClient(_ context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	region := d.Get("region").(string)
	sess, err := session.NewSession(&aws.Config{
		Region: &region,
	})
	if err != nil {
		return nil, diag.FromErr(err)
	}

	return lambda.New(sess), nil
}
