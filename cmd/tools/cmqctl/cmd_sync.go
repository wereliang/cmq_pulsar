package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wereliang/cmq_pulsar/pkg/api"
)

const (
	TopicFile     = "./cmq.topic"
	QueueFile     = "./cmq.queue"
	SubscribeFile = "./cmq.subscribe"
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "sync tool",
	Long:  `sync tool ...`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var syncDumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "dump cmq",
	Long:  `dump cmq meta (must export env: SecretID and SecretKey)`,
	Run: func(cmd *cobra.Command, args []string) {
		dump()
	},
}

var syncLoadCmd = &cobra.Command{
	Use:   "load",
	Short: "load cmq",
	Long:  `load cmq meta (must export env: SecretID and SecretKey)`,
	Run: func(cmd *cobra.Command, args []string) {
		load()
	},
}

var syncAnalyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "analyze cmq",
	Long:  `analyze topic`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("input topic")
			return
		}
		analyze(args[0])
	},
}

// 同步
var syncSyncCmd = &cobra.Command{
	Use:   "sync",
	Short: "[topic]",
	Long:  `[topic]`,
	Run: func(cmd *cobra.Command, args []string) {
		var topic string
		if len(args) > 0 {
			// fmt.Println("input topic")
			topic = args[0]
		}
		doSync(topic)
		// analyze(args[0])
		// var yesorno string
		// fmt.Print("continue to sync? [yes|no] : ")
		// fmt.Scanln(&yesorno)
		// if yesorno == "yes" {
		// 	fmt.Println("start ....")
		// }
	},
}

func init() {
	rootCmd.AddCommand(syncCmd)
	syncCmd.AddCommand(syncDumpCmd)
	syncCmd.AddCommand(syncLoadCmd)
	syncCmd.AddCommand(syncAnalyzeCmd)
	syncCmd.AddCommand(syncSyncCmd)
}

func dumpTopic(topics []api.TopicObject) {
	tempFile := TopicFile + ".temp"
	file, err := os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("文件打开失败", err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, topic := range topics {
		writer.WriteString(topic.TopicName + "\n")
	}

	writer.Flush()
	err = os.Rename(tempFile, TopicFile)
	if err != nil {
		fmt.Println("rename file error.", err)
		return
	}
	fmt.Println("dump topic success!")
}

func dumpQueue(queues []api.QueueObject) {
	tempFile := QueueFile + ".temp"
	file, err := os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("文件打开失败", err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, queue := range queues {
		writer.WriteString(queue.QueueName + "\n")
	}

	writer.Flush()
	err = os.Rename(tempFile, QueueFile)
	if err != nil {
		fmt.Println("rename file error.", err)
		return
	}
	fmt.Println("dump queue success!")
}

func dumpSubscribe(subscribes map[string][]api.SubscribeObject) {
	tempFile := SubscribeFile + ".temp"
	file, err := os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("文件打开失败", err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for topic, subs := range subscribes {
		for _, sub := range subs {
			if sub.Protocol != "queue" {
				fmt.Printf("not support %#v\n", sub)
				continue
			}
			writer.WriteString(sub.SubscriptionName + " " + topic + " " + sub.Endpoint + "\n")
		}
	}

	writer.Flush()
	err = os.Rename(tempFile, SubscribeFile)
	if err != nil {
		fmt.Println("rename file error.", err)
		return
	}
	fmt.Println("dump subscribe success!")
}

func dump() {
	cmqApi := newCmqApi()
	offset, limit := 0, 100

	var queues []api.QueueObject
	for {
		request := &api.ListQueueRequest{Offset: offset, Limit: limit}
		response, err := cmqApi.ListQueue(request)
		if err != nil {
			fmt.Println("list queue error", err)
			return
		}
		if response.Code != 0 {
			fmt.Printf("list queue code error. %#v\n", response)
			return
		}
		queues = append(queues, response.QueueList...)
		// fmt.Printf("%#v\n", response)
		if len(response.QueueList) < limit {
			break
		}
		offset += limit
	}

	offset, limit = 0, 100
	var topics []api.TopicObject
	for {
		request := &api.ListTopicRequest{Offset: offset, Limit: limit}
		response, err := cmqApi.ListTopic(request)
		if err != nil {
			fmt.Println("list topic error", err)
			return
		}
		if response.Code != 0 {
			fmt.Printf("list topic code error. %#v\n", response)
			return
		}
		topics = append(topics, response.TopicList...)
		// fmt.Printf("%#v\n", response)
		if len(response.TopicList) < limit {
			break
		}
		offset += limit
	}

	subscribes := make(map[string][]api.SubscribeObject)
	for _, t := range topics {
		request := &api.ListSubscribeRequest{TopicName: t.TopicName}
		response, err := cmqApi.ListSubscribe(request)
		if err != nil {
			fmt.Println("list subscribe error", err)
			return
		}
		subscribes[t.TopicName] = response.SubscriptionList
	}

	dumpTopic(topics)
	dumpQueue(queues)
	dumpSubscribe(subscribes)
}

func load() (map[string][]api.SubscribeObject, map[string][]string, error) {
	f, err := os.Open(SubscribeFile)
	if err != nil {
		fmt.Println("open file error. ", err)
		return nil, nil, err
	}

	subscribes := make(map[string][]api.SubscribeObject) // topic -> subs
	queues := make(map[string][]string)                  // queue -> topics
	br := bufio.NewReader(f)
	for {
		a, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		// fmt.Println(string(a))
		objs := strings.Split(string(a), " ")
		name := objs[0]
		topic := objs[1]
		queue := objs[2]
		subscribes[topic] = append(subscribes[topic], api.SubscribeObject{SubscriptionName: name, Endpoint: queue})
		queues[queue] = append(queues[queue], topic)
	}

	// fmt.Printf("subscribe: %#v\n", subscribes)
	// fmt.Printf("queues: %#v\n", queues)
	return subscribes, queues, nil
}

func __analyze(subscribes map[string][]api.SubscribeObject, queues map[string][]string, topic string, topics map[string]interface{}) {
	topics[topic] = true
	if subs, ok := subscribes[topic]; ok {
		fmt.Println(topic)
		for _, sub := range subs {
			fmt.Printf("\t%s\n", sub.Endpoint)
		}

		for _, sub := range subs {
			if tps, ok := queues[sub.Endpoint]; ok {
				for _, t := range tps {
					if _, ok := topics[t]; ok {
						continue
					}
					__analyze(subscribes, queues, t, topics)
				}
			}
		}
	}
}

func analyze(topic string) {
	subs, queues, err := load()
	if err != nil {
		panic(err)
	}

	topics := make(map[string]interface{})
	__analyze(subs, queues, topic, topics)
	// fmt.Println(topics)
}

func doSync(topic string) {
	subs, queues, err := load()
	if err != nil {
		panic(err)
	}

	var yesorno string
	if topic != "" {
		topics := make(map[string]interface{})
		__analyze(subs, queues, topic, topics)

		fmt.Print("continue to sync? [yes|no] : ")
		fmt.Scanln(&yesorno)
		if yesorno != "yes" {
			return
		}

		cmqApi := newCmqApi()
		for t, _ := range topics {
			var subscriptions []api.SubscribeObject
			var ok bool
			if subscriptions, ok = subs[t]; !ok {
				panic("not found topic " + t)
			}

			resp, err := cmqApi.CreateTopic(&api.CreateTopicRequest{TopicName: t})
			if err != nil || resp.Code != 0 {
				fmt.Printf("Topic [%s] Error. %#v %#v", t, err, resp)
				continue
			}
			fmt.Printf("Topic [%s] OK.\n", t)

			for _, sub := range subscriptions {
				resp, err := cmqApi.CreateQueue(&api.CreateQueueRequest{QueueName: sub.Endpoint})
				if err != nil || resp.Code != 0 {
					fmt.Printf("\tQueue [%s] Error. %#v %#v", sub.Endpoint, err, resp)
				} else {
					fmt.Printf("\tQueue [%s] OK.\n", sub.Endpoint)
				}

				resp2, err := cmqApi.CreateSubscribe(
					&api.CreateSubscribeRequest{
						TopicName:        t,
						SubscriptionName: sub.SubscriptionName,
						Endpoint:         sub.Endpoint})
				if err != nil || resp2.Code != 0 {
					fmt.Printf("\tSubscribe [%s] Error. %#v %#v\n", sub.SubscriptionName, err, resp2)
				} else {
					fmt.Printf("\tSubscribe [%s] OK.\n", sub.SubscriptionName)
				}

			}
		}

	} else {
		fmt.Printf("continue to sync all [%d] subscribe. [yes|no] :", len(subs))
		fmt.Scanln(&yesorno)
		if yesorno != "yes" {
			return
		}

		cmqApi := newCmqApi()
		for t, subscriptions := range subs {
			resp, err := cmqApi.CreateTopic(&api.CreateTopicRequest{TopicName: t})
			if err != nil || resp.Code != 0 {
				fmt.Printf("Topic [%s] Error. %#v %#v\n", t, err, resp)
				continue
			}
			fmt.Printf("Topic [%s] OK.\n", t)

			for _, sub := range subscriptions {

				// fmt.Printf("\tQueue [%s] OK.\n", sub.Endpoint)
				// fmt.Printf("\tSubscribe [%s] OK.\n", sub.SubscriptionName)
				resp, err := cmqApi.CreateQueue(&api.CreateQueueRequest{QueueName: sub.Endpoint})
				if err != nil || resp.Code != 0 {
					fmt.Printf("\tQueue [%s] Error. %#v %#v\n", sub.Endpoint, err, resp)
				} else {
					fmt.Printf("\tQueue [%s] OK.\n", sub.Endpoint)
				}

				resp2, err := cmqApi.CreateSubscribe(
					&api.CreateSubscribeRequest{
						TopicName:        t,
						SubscriptionName: sub.SubscriptionName,
						Endpoint:         sub.Endpoint})
				if err != nil || resp2.Code != 0 {
					fmt.Printf("\tSubscribe [%s] Error. %#v %#v\n", sub.SubscriptionName, err, resp2)
				} else {
					fmt.Printf("\tSubscribe [%s] OK.\n", sub.SubscriptionName)
				}
			}
		}
	}

}
