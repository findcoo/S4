package main

import (
	"errors"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/findcoo/S4/test"
	"github.com/urfave/cli"
)

var (
	// ErrOptionRequired require option
	ErrOptionRequired = errors.New("some options required, check up help")
	s3ConfigFlag      = []cli.Flag{
		cli.StringFlag{
			Name:   "s3Path, s",
			Usage:  "s3 path, required",
			EnvVar: "S4_S3_PATH",
		},
		cli.StringFlag{
			Name:   "region, r",
			Usage:  "aws s3 region, required",
			EnvVar: "S4_REGION",
		},
	}
	bufferConfigFlag = []cli.Flag{
		cli.StringFlag{
			Name:   "buffer, b",
			Value:  "./buffer.db",
			Usage:  "buffer database path",
			EnvVar: "S4_BUFFER_PATH",
		},
		cli.StringFlag{
			Name:   "unix, u",
			Usage:  "unix file socket path, required",
			EnvVar: "S4_SOCKET_PATH",
		},
		cli.DurationFlag{
			Name:   "flush, f",
			Value:  time.Minute * 5,
			Usage:  "flush time interval",
			EnvVar: "S4_FLUSH_TIME",
		},
	}
)

func s4OptionParser(c *cli.Context) (*S4Config, error) {
	bufferPath := c.String("buffer")
	socketPath := c.String("unix")
	if socketPath == "" {
		return nil, ErrOptionRequired
	}
	s3Path := c.String("s3Path")
	if s3Path == "" {
		return nil, ErrOptionRequired
	}
	region := c.String("region")
	if region == "" {
		return nil, ErrOptionRequired
	}
	flush := c.Duration("flush")
	bucket, key := path.Split(s3Path)
	bucket = strings.TrimRight(bucket, "/")

	config := &S4Config{
		AWSRegion:         region,
		S3Bucket:          bucket,
		S3Key:             key,
		FlushIntervalTime: flush,
		BufferPath:        bufferPath,
		SocketPath:        socketPath,
	}

	return config, nil
}

func s4Client(c *cli.Context) error {
	config, err := s4OptionParser(c)
	if err != nil {
		return err
	}

	s4 := NewS4(config)
	s4.ClientBufferProducer()
	s4.BufferConsumer().Subscribe(func(data []byte) {
		if err := s4.SendToS3(data); err != nil {
			log.Print(err)
		}
	})
	return nil
}

func s4Server(c *cli.Context) error {
	config, err := s4OptionParser(c)
	if err != nil {
		return err
	}

	s4 := NewS4(config)
	s4.ServerBufferProducer()
	s4.BufferConsumer().Subscribe(func(data []byte) {
		if err := s4.SendToS3(data); err != nil {
			log.Print(err)
		}
	})
	return nil
}

func mockingTest(c *cli.Context) error {
	go test.MockUnixEchoServer(time.Second * 10)

	config := &S4Config{
		FlushIntervalTime: time.Second * 1,
		BufferPath:        "./mock.db",
	}

	s4 := NewS4(config)
	s4.ClientBufferProducer()
	deadline := time.After(time.Second * 10)
	buffer := s4.BufferConsumer()
	buffer.Subscribe(func(data []byte) {
		select {
		case <-deadline:
			buffer.Cancel()
		default:
			log.Print(data)
		}
	})
	return nil
}

// NewApp new CLI app
func NewApp() *cli.App {
	app := cli.NewApp()

	app.Commands = []cli.Command{
		{
			Name:    "mock",
			Aliases: []string{"m"},
			Usage:   "running mock unix socket server",
			Action:  mockingTest,
		},
		{
			Name:    "client",
			Flags:   append(s3ConfigFlag, bufferConfigFlag...),
			Aliases: []string{"c"},
			Usage:   "connect unix socket and stream to s3",
			Action:  s4Client,
		},
		{
			Name:    "server",
			Flags:   append(s3ConfigFlag, bufferConfigFlag...),
			Aliases: []string{"s"},
			Usage:   "listen connection and stream to s3",
			Action:  s4Server,
		},
	}

	app.Name = "s4"
	app.Usage = "Simple Storage Service Stream"
	app.Version = "1.0.4"
	return app
}

func main() {
	app := NewApp()
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
