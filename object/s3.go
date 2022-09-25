package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type Handler struct {
	ctx          context.Context
	client       *s3.Client
	requestPayer string
}

type Option func(o *Handler)

func Client(cl *s3.Client) Option {
	return func(o *Handler) {
		o.client = cl
	}
}

func RequestPayer() Option {
	return func(o *Handler) {
		o.requestPayer = "requester"
	}
}

func Handle(ctx context.Context, opts ...Option) (*Handler, error) {
	handler := &Handler{
		ctx: ctx,
	}
	for _, o := range opts {
		o(handler)
	}
	if handler.client == nil {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 client: %w", err)
		}
		handler.client = s3.NewFromConfig(cfg)
	}
	return handler, nil
}

func handleS3ApiError(err error) (io.ReadCloser, int64, error) {
	var ae smithy.APIError
	if errors.As(err, &ae) && ae.ErrorCode() == "InvalidRange" {
		return nil, 0, io.EOF
	}
	if errors.As(err, &ae) && (ae.ErrorCode() == "NoSuchBucket" || ae.ErrorCode() == "NoSuchKey" || ae.ErrorCode() == "NotFound") {
		return nil, -1, syscall.ENOENT
	}
	return nil, 0, err
}

func (h *Handler) StreamAt(key string, off int64, n int64) (io.ReadCloser, int64, error) {
	bucket, object, err := BucketObject(key)
	if err != nil {
		return nil, 0, err
	}
	log.Printf("bucket:%s object:%s\n", bucket, object)
	var size int64
	if off == 0 {
		r, err := h.client.HeadObject(h.ctx, &s3.HeadObjectInput{
			Bucket:       &bucket,
			Key:          &object,
			RequestPayer: types.RequestPayer(h.requestPayer),
		})
		if err != nil {
			return handleS3ApiError(fmt.Errorf("new reader for s3://%s/%s: %w", bucket, object, err))
		}
		size = r.ContentLength
	}

	r, err := h.client.GetObject(h.ctx, &s3.GetObjectInput{
		Bucket:       &bucket,
		Key:          &object,
		RequestPayer: types.RequestPayer(h.requestPayer),
		Range:        aws.String(fmt.Sprintf("bytes=%d-%d", off, off+n-1)),
	})
	if err != nil {
		return handleS3ApiError(fmt.Errorf("new reader for s3://%s/%s: %w", bucket, object, err))
	}
	return r.Body, size, err
}

func BucketObject(input string) (string, string, error) {
	parse, err := url.Parse(input)
	if err != nil {
		return "", "", err
	}

	return parse.Host, parse.Path, err
}
