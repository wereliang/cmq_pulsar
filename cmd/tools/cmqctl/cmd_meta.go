package main

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/wereliang/cmq_pulsar/pkg/api"
)

var metaCmd = &cobra.Command{
	Use:   "meta",
	Short: "meta tool",
	Long:  `meta tool ...`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var metaCreateTopicCmd = &cobra.Command{
	Use:   "createtopic",
	Short: "topic",
	Long:  `topic`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("please input topicname")
			return
		}
		createTopic(args[0])
	},
}

var metaCreateQueueCmd = &cobra.Command{
	Use:   "createqueue",
	Short: "queue",
	Long:  `queue`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("please input queuename")
			return
		}
		createQueue(args[0])
	},
}

var metaCreateSubscribeCmd = &cobra.Command{
	Use:   "createsubscribe",
	Short: "topic queue",
	Long:  `topic queue`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			fmt.Println("please input topic queue")
			return
		}
		createSubscribe(args[0], args[1])
	},
}

var metaUnsubscribeCmd = &cobra.Command{
	Use:   "unsubscribe",
	Short: "name topic",
	Long:  `name topic`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			fmt.Println("please input subscribe topic")
			return
		}
		unsubscribe(args[0], args[1])
	},
}

var metaListTopicCmd = &cobra.Command{
	Use:   "listtopic",
	Short: "[offset] [limit]",
	Long:  `[offset] [limit]`,
	Run: func(cmd *cobra.Command, args []string) {
		offset, limit := -1, -1
		if len(args) > 0 {
			offset, _ = strconv.Atoi(args[0])
		}
		if len(args) > 1 {
			limit, _ = strconv.Atoi(args[1])
		}
		listTopic(offset, limit)
	},
}

var metaListQueueCmd = &cobra.Command{
	Use:   "listqueue",
	Short: "[offset] [limit]",
	Long:  `[offset] [limit]`,
	Run: func(cmd *cobra.Command, args []string) {
		offset, limit := -1, -1
		if len(args) > 0 {
			offset, _ = strconv.Atoi(args[0])
		}
		if len(args) > 1 {
			limit, _ = strconv.Atoi(args[1])
		}
		listQueue(offset, limit)
	},
}

var metaListSubscribeCmd = &cobra.Command{
	Use:   "listsubscribe",
	Short: "topic",
	Long:  `topic`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("please input topic")
			return
		}
		listSubscribe(args[0])
	},
}

func init() {
	rootCmd.AddCommand(metaCmd)
	metaCmd.AddCommand(metaListSubscribeCmd)
	metaCmd.AddCommand(metaListTopicCmd)
	metaCmd.AddCommand(metaListQueueCmd)
	metaCmd.AddCommand(metaCreateTopicCmd)
	metaCmd.AddCommand(metaCreateQueueCmd)
	metaCmd.AddCommand(metaCreateSubscribeCmd)
	metaCmd.AddCommand(metaUnsubscribeCmd)
}

func listSubscribe(topic string) {
	cmq := newCmqApi()
	resp, err := cmq.ListSubscribe(&api.ListSubscribeRequest{TopicName: topic})
	if err != nil {
		panic(err)
	}
	// fmt.Printf("%#v\n", resp)
	printObject(resp)
}

func listTopic(offset, limit int) {
	cmqApi := newCmqApi()
	if offset != -1 {
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
		// fmt.Printf("%#v\n", response)
		printObject(response)
	} else {
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
			printObject(response)
			if len(response.TopicList) < limit {
				break
			}
			offset += limit
		}
	}
}

func listQueue(offset, limit int) {
	cmqApi := newCmqApi()
	if offset != -1 {
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
		// fmt.Printf("%#v\n", response)
		printObject(response)
	} else {
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
			printObject(response)
			if len(response.QueueList) < limit {
				break
			}
			offset += limit
		}
	}
}

func createTopic(name string) {
	cmqApi := newCmqApi()
	request := &api.CreateTopicRequest{TopicName: name}
	response, err := cmqApi.CreateTopic(request)
	if err != nil {
		fmt.Println("error.", err)
		return
	}
	fmt.Printf("%#v\n", response)
}

func createQueue(name string) {
	cmqApi := newCmqApi()
	request := &api.CreateQueueRequest{QueueName: name}
	response, err := cmqApi.CreateQueue(request)
	if err != nil {
		fmt.Println("error.", err)
		return
	}
	fmt.Printf("%#v\n", response)
}

func createSubscribe(topic, queue string) {
	cmqApi := newCmqApi()
	request := &api.CreateSubscribeRequest{
		SubscriptionName: fmt.Sprintf("%s-SUB-%s", queue, topic),
		TopicName:        topic,
		Endpoint:         queue,
		Protocol:         "queue",
	}
	response, err := cmqApi.CreateSubscribe(request)
	if err != nil {
		fmt.Println("error.", err)
		return
	}
	fmt.Printf("%#v\n", response)
}

func unsubscribe(name string, topic string) {
	cmqApi := newCmqApi()
	request := &api.UnsubscribeRequest{
		TopicName:        topic,
		SubscriptionName: name,
	}
	response, err := cmqApi.Unsubscribe(request)
	if err != nil {
		fmt.Println("error.", err)
		return
	}
	fmt.Printf("%#v\n", response)
}
