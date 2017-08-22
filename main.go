package main

import (
	"errors"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/findcoo/S4/lake"
	"github.com/findcoo/S4/river"
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
			Value:  "./tmp",
			Usage:  "path of the file buffer",
			EnvVar: "S4_BUFFER_PATH",
		},
		cli.StringFlag{
			Name:   "unix, u",
			Usage:  "path of the unix socket",
			EnvVar: "S4_SOCKET_PATH",
		},
		cli.DurationFlag{
			Name:   "flush, f",
			Value:  time.Minute * 5,
			Usage:  "flush time interval",
			EnvVar: "S4_FLUSH_TIME",
		},
		cli.StringFlag{
			Name:   "type, t",
			Value:  "line",
			Usage:  "define the buffer type that can be parsed format(json, line)",
			EnvVar: "S4_RIVER_TYPE",
		},
	}
)

func optionParser(c *cli.Context) (*river.Config, error) {
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

	s3lake := lake.NewS3Supplyer(region, bucket, key)

	config := &river.Config{
		BufferPath:        bufferPath,
		SocketPath:        socketPath,
		FlushIntervalTime: flush,
		Supplyer:          s3lake,
	}
	return config, nil
}

func connect(r river.River) {
	r.Connect()
	r.Consume().Subscribe(func(data []byte) {
		if err := r.Push(data); err != nil {
			log.Print(err)
		}
	})
}

func listen(r river.River) {
	r.Listen()
	r.Consume().Subscribe(func(data []byte) {
		if err := r.Push(data); err != nil {
			log.Print(err)
		}
	})
}

func s4Client(c *cli.Context) error {
	config, err := optionParser(c)
	if err != nil {
		return err
	}
	rivername := c.String("type")

	switch rivername {
	case "line":
		liner := river.NewLineRiver(config)
		connect(liner)
	case "json":
		jsonr := river.NewJSONRiver(config)
		connect(jsonr)
	}
	return nil
}

func s4Server(c *cli.Context) error {
	config, err := optionParser(c)
	if err != nil {
		return err
	}
	rivername := c.String("type")

	switch rivername {
	case "line":
		liner := river.NewLineRiver(config)
		listen(liner)
	case "json":
		jsonr := river.NewJSONRiver(config)
		listen(jsonr)
	}
	return nil
}

func mockingTest(c *cli.Context) error {
	done := test.MockUnixEchoServer()

	config := &river.Config{
		FlushIntervalTime: time.Second * 1,
		BufferPath:        "./mock.db",
		SocketPath:        "./mock.sock",
		Supplyer:          lake.NewConsoleSupplyer(),
	}

	river := river.NewJSONRiver(config)
	river.Connect()
	deadline := time.After(time.Second * 10)
	consumer := river.Consume()
	consumer.Subscribe(func(data []byte) {
		select {
		case <-deadline:
			done <- struct{}{}
			consumer.Cancel()
		default:
			log.Print(string(data))
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
	app.Version = "2.1.1"
	return app
}

func main() {
	app := NewApp()
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
