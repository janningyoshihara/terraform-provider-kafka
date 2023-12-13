package kafka

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var testProvider, _ = overrideProvider()

func TestProvider(t *testing.T) {
	if err := Provider().InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func overrideProviderFactory() map[string]func() (*schema.Provider, error) {
	return map[string]func() (*schema.Provider, error){
		"kafka": func() (*schema.Provider, error) {
			return overrideProvider()
		},
	}
}

func overrideProvider() (*schema.Provider, error) {
	log.Println("[INFO] Setting up override for a provider")
	provider := Provider()

	rc, err := accTestProviderConfig()
	if err != nil {
		return nil, err
	}
	diags := provider.Configure(context.Background(), rc)
	if diags.HasError() {
		log.Printf("[ERROR] Could not configure provider %v", diags)
		return nil, fmt.Errorf("Could not configure provider")
	}

	return provider, nil
}

func accTestProviderConfig() (*terraform.ResourceConfig, error) {

	raw := map[string]interface{}{}

	return terraform.NewResourceConfigRaw(raw), nil
}
