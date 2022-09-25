package frio

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s32 "github.com/ruanlianjun/frio/object"
)

var (
	ak       = ""
	sk       = ""
	region   = ""
	endpoint = ""
)

func client() *Adapter {
	options := aws.EndpointResolverWithOptionsFunc(func(service, region string,
		options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               endpoint, // or where ever you ran minio
			SigningRegion:     region,
			HostnameImmutable: true,
		}, nil
	})

	cfg := aws.Config{
		Region:                      region,
		Credentials:                 credentials.NewStaticCredentialsProvider(ak, sk, ""),
		EndpointResolverWithOptions: options,
	}
	s3cl := s3.NewFromConfig(cfg)

	s3r, err := s32.Handle(context.Background(), s32.Client(s3cl))
	if err != nil {
		panic(err)
	}
	adapter := NewAdapter(s3r, WithDataCache(10000))
	return adapter
}

func TestRead(t *testing.T) {
	reader := client().Reader("s3://pie-engine-test/demo.txt")

	bt, n, err := reader.Read()
	if err != nil {
		panic(err)
	}

	fmt.Println("===========", string(bt), n)
}
