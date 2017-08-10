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
	// ErrSocketPathRequired require unix socket path
	ErrSocketPathRequired = errors.New("-u, --unix option required")
	// ErrS3PathRequired require s3 path
	ErrS3PathRequired = errors.New("-s, --s3Path option required")
	// ErrS3RegionRequired require s3 region name
	ErrS3RegionRequired = errors.New("-r, --region option required")
	s3ConfigFlag        = []cli.Flag{
		cli.StringFlag{
			Name:   "s3Path, s",
			Usage:  "s3 path",
			EnvVar: "S4_S3_PATH",
		},
		cli.StringFlag{
			Name:   "region, r",
			Usage:  "aws s3 region",
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
			Usage:  "unix file socket path",
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

func s4Handler(c *cli.Context) error {
	bufferPath := c.String("buffer")
	socketPath := c.String("unix")
	if socketPath == "" {
		return ErrSocketPathRequired
	}
	s3Path := c.String("s3Path")
	region := c.String("region")
	flush := c.Duration("flush")
	bucket, key := path.Split(s3Path)
	bucket = strings.TrimRight(bucket, "/")

	config := &S4Config{
		AWSRegion:         region,
		S3Bucket:          bucket,
		S3Key:             key,
		FlushIntervalTime: flush,
	}
	s4 := NewS4(bufferPath, config)
	s4.BufferProducer(socketPath)
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
	}

	s4 := NewS4("./mock.db", config)
	s4.BufferProducer("./mock.sock")
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

	app.Flags = append(s3ConfigFlag, bufferConfigFlag...)
	app.Commands = []cli.Command{
		{
			Name:    "mock",
			Aliases: []string{"m"},
			Usage:   "running mock unix socket server",
			Action:  mockingTest,
		},
	}

	app.Name = "s4"
	app.Usage = "Simple Storage Service Stream"
	app.Action = s4Handler
	return app
}

func main() {
	app := NewApp()
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
