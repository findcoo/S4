package main

import (
	"errors"
	"log"
	"os"
	"path"
	"time"

	"github.com/findcoo/rxs3/test"
	"github.com/urfave/cli"
)

var (
	// ErrSocketPathRequired require unix socket path
	ErrSocketPathRequired = errors.New("-u, --unix option required")
	// ErrS3PathRequired require s3 path
	ErrS3PathRequired = errors.New("-s, --s3Path option required")
	// ErrS3RegionRequired require s3 region name
	ErrS3RegionRequired = errors.New("-r, --region option required")
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

	config := &Config{
		AWSRegion:         region,
		S3Bucket:          bucket,
		S3Key:             key,
		FlushIntervalTime: flush,
	}
	s4 := NewRxS3(bufferPath, config)
	s4.BufferProducer(socketPath)
	s4.BufferConsumer()
	return nil
}

// NewApp new CLI app
func NewApp() *cli.App {
	app := cli.NewApp()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "buffer, b",
			Value:  "./buffer.db",
			Usage:  "buffer database path, default: ./buffer.db",
			EnvVar: "S4_BUFFER_PATH",
		},
		cli.StringFlag{
			Name:   "unix, u",
			Usage:  "unix file socket path",
			EnvVar: "S4_SOCKET_PATH",
		},
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
		cli.DurationFlag{
			Name:   "flush, f",
			Value:  time.Minute * 5,
			Usage:  "flush time interval, example formats (5m, 5h24m, 5s)",
			EnvVar: "S4_FLUSH_TIME",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "mock",
			Aliases: []string{"m"},
			Usage:   "running mock unix socket server",
			Action: func(c *cli.Context) error {
				test.MockUnixEchoServer(time.Second * 20)
				return nil
			},
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
