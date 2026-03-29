package authgrpc

import (
	"context"
	"time"

	authpb "singularity.com/pr14/proto/authpb"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Client struct {
	conn   *grpc.ClientConn
	client authpb.AuthServiceClient
	log    *logrus.Entry
}

func New(addr string, log *logrus.Logger) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:   conn,
		client: authpb.NewAuthServiceClient(conn),
		log: log.WithFields(logrus.Fields{
			"service":   "tasks",
			"component": "auth_client",
		}),
	}, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) Verify(ctx context.Context, token string, requestID string) (*authpb.VerifyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if requestID != "" {
		md := metadata.Pairs("x-request-id", requestID)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	c.log.WithFields(logrus.Fields{
		"request_id": requestID,
		"has_auth":   token != "",
	}).Info("calling grpc verify")

	resp, err := c.client.Verify(ctx, &authpb.VerifyRequest{
		Token: token,
	})

	if err != nil {
		c.log.WithFields(logrus.Fields{
			"request_id": requestID,
			"error":      err.Error(),
		}).Error("grpc verify failed")
		return nil, err
	}

	c.log.WithFields(logrus.Fields{
		"request_id": requestID,
		"valid":      resp.GetValid(),
	}).Info("grpc verify completed")

	return resp, nil
}
