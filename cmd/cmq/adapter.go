package main

import (
	"fmt"
	"io/ioutil"

	"github.com/wereliang/cmq_pulsar/pkg/api"
	"github.com/wereliang/cmq_pulsar/pkg/cpadapter"

	// plog "github.com/apache/pulsar-client-go/pulsar/log"
	yaml "gopkg.in/yaml.v2"
)

var (
	adapter cpadapter.CmqPulsarAdapter
	builder api.CMQRequestBuilder
)

func NewAdapter(cfg string) error {
	var config cpadapter.Config
	yamlFile, err := ioutil.ReadFile(cfg)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%#v\n", config)

	// config.Logger = plog.DefaultNopLogger()

	// mylog := &logrus.Logger{
	// 	Out:          os.Stderr,
	// 	Formatter:    new(logrus.TextFormatter),
	// 	Hooks:        make(logrus.LevelHooks),
	// 	Level:        logrus.DebugLevel,
	// 	ExitFunc:     os.Exit,
	// 	ReportCaller: false,
	// }
	// config.Logger = plog.NewLoggerWithLogrus(mylog)

	adapter, err = cpadapter.NewCmqPulsarAdapterWithConfig(&config)
	if err != nil {
		panic(err)
	}
	return nil
}

func Meta() cpadapter.CmqPulsarMetaServer {
	return adapter.Meta()
}

func Producer() cpadapter.CmqPulsarProducer {
	return adapter.Producer()
}

func Consumer() cpadapter.CmqPulsarConsumer {
	return adapter.Consumer()
}

func Builder() api.CMQRequestBuilder {
	return builder
}

func init() {
	// just use parser so config nothing
	builder = api.NewCMQRequestBuilder(api.CmqConfig{})
}
