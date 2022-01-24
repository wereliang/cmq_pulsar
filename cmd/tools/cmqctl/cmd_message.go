package main

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/wereliang/cmq_pulsar/pkg/api"
)

var msgCmd = &cobra.Command{
	Use:   "message",
	Short: "message tool",
	Long:  `message tool ...`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var msgReceiveCmd = &cobra.Command{
	Use:   "receive",
	Short: "queue seconds [delete:1|0]",
	Long:  `queue seconds [delete:1|0]`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			fmt.Println("please input queue seconds [delete:1|0]")
			return
		}
		del := true
		if len(args) > 2 && args[2] == "0" {
			del = false
		}
		seconds, _ := strconv.Atoi(args[1])
		receive(args[0], seconds, del)
	},
}

var msgBatchReceiveCmd = &cobra.Command{
	Use:   "batchreceive",
	Short: "queue seconds number [delete:1|0]",
	Long:  `queue seconds number [delete:1|0]`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 3 {
			fmt.Println("please input queue seconds number [delete:1|0]")
			return
		}
		del := true
		if len(args) > 3 && args[3] == "0" {
			del = false
		}
		seconds, _ := strconv.Atoi(args[1])
		number, _ := strconv.Atoi(args[2])
		batchReceive(args[0], seconds, number, del)
	},
}

var msgDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "queue msgid",
	Long:  `queue msgid`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			fmt.Println("please input queue msgid")
			return
		}
		deleteMessage(args[0], args[1], nil)
	},
}

var msgPublishCmd = &cobra.Command{
	Use:   "publish",
	Short: "topic msg",
	Long:  `topic msg`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			fmt.Println("please input topic msg")
			return
		}
		publishMessage(args[0], args[1])
	},
}

var msgBatchPublishCmd = &cobra.Command{
	Use:   "batchpublish",
	Short: "topic msg1,msg2,msg3...",
	Long:  `topic msg1,msg2,msg3...`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			fmt.Println("please input topic msg")
			return
		}
		msgs := strings.Split(args[1], ",")
		batchPublishMessage(args[0], msgs)
	},
}

var msgBenchCmd = &cobra.Command{
	Use:   "bench",
	Short: "[produc thread] [consume thread] [message number] [topic]",
	Long:  `[produc thread] [consume thread] [message number] [topic]`,
	Run: func(cmd *cobra.Command, args []string) {
		pthread, cthread, number, topic := 2, 10, 100, ""
		if len(args) > 0 {
			pthread, _ = strconv.Atoi(args[0])
		}
		if len(args) > 1 {
			cthread, _ = strconv.Atoi(args[1])
		}
		if len(args) > 2 {
			number, _ = strconv.Atoi(args[2])
		}
		if len(args) > 3 {
			topic = args[3]
		}
		bench(pthread, cthread, number, topic)
	},
}

func init() {
	rootCmd.AddCommand(msgCmd)
	msgCmd.AddCommand(msgReceiveCmd)
	msgCmd.AddCommand(msgDeleteCmd)
	msgCmd.AddCommand(msgPublishCmd)
	msgCmd.AddCommand(msgBatchPublishCmd)
	msgCmd.AddCommand(msgBatchReceiveCmd)
	msgCmd.AddCommand(msgBenchCmd)
}

func receive(queue string, seconds int, del bool) {
	cmqApi := newCmqApi(&http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          10,
		MaxConnsPerHost:       10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	})

	for {
		t1 := time.Now()
		resp, err := cmqApi.ReceiveMessage(&api.ReceiveMessageRequest{
			QueueName:          queue,
			PollingWaitSeconds: seconds,
		})
		if err != nil {
			fmt.Println("receive error.", err)
			break
		}
		cos := time.Since(t1)
		if resp.Code != 0 {
			fmt.Printf("%s ReceiptHandle:%s cos:%f\n", resp.Message, resp.RequestId, cos.Seconds())
			continue
		} else {
			fmt.Printf("body:%s msgId:%s ReceiptHandle:%s dequeue:%d cos:%f\n",
				resp.MsgBody, resp.MsgId, resp.ReceiptHandle, resp.DequeueCount, cos.Seconds())
		}

		if del {
			deleteMessage(queue, resp.ReceiptHandle, cmqApi)
		}
	}
}

func deleteMessage(queue string, receiptHandle string, cmqApi api.CmqApi) {
	if cmqApi == nil {
		cmqApi = newCmqApi()
	}

	resp, err := cmqApi.DeleteMessage(&api.DeleteMessageRequest{
		QueueName:     queue,
		ReceiptHandle: receiptHandle,
	})
	if err != nil || resp.Code != 0 {
		fmt.Printf("delete message error. %#v %#v", err, resp)
		return
	}
	fmt.Println("delete ok. ", queue, receiptHandle)
}

func publishMessage(topic string, msg string) {
	cmqApi := newCmqApi(produceTransport)
	resp, err := cmqApi.PublishMessage(&api.PublishMessageRequest{
		TopicName: topic,
		MsgBody:   msg,
	})
	if err != nil || resp.Code != 0 {
		fmt.Printf("publish error. %#v %#v", err, resp)
		return
	}
	fmt.Printf("publish ok! %s %s\n", topic, msg)
}

func batchPublishMessage(topic string, msgs []string) {
	cmqApi := newCmqApi()
	resp, err := cmqApi.BatchPublishMessage(&api.BatchPublishMessageRequest{
		TopicName: topic,
		MsgBody:   msgs,
	})
	if err != nil || resp.Code != 0 {
		fmt.Printf("publish error. %#v %#v", err, resp)
		return
	}
	fmt.Println("publish ok!")
}

func batchReceive(queue string, seconds int, number int, del bool) {
	cmqApi := newCmqApi()
	for {
		resp, err := cmqApi.BatchReceiveMessage(&api.BatchReceiveMessageRequest{
			QueueName:          queue,
			PollingWaitSeconds: seconds,
			NumOfMsg:           number,
		})
		if err != nil {
			fmt.Println("receive error.", err)
			break
		}
		if resp.Code != 0 {
			fmt.Println(resp.Message)
			continue
		} else {
			for _, msg := range resp.MsgInfoList {
				fmt.Printf("body:%s msgId:%s ReceiptHandle:%s dequeue:%d\n",
					msg.MsgBody, msg.MsgId, msg.ReceiptHandle, msg.DequeueCount)
				if del {
					deleteMessage(queue, msg.ReceiptHandle, cmqApi)
				}
			}
		}

	}
}

var produceTransport *http.Transport

func bench(pthread, cthread int, number int, topic string) {
	subs, _, err := load()
	if err != nil {
		return
	}

	produceTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          10240,
		MaxConnsPerHost:       10240,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	var wg sync.WaitGroup
	if topic == "" {
		for topic, subscriptions := range subs {
			fmt.Printf("--- topic:%s %#v\n", topic, subscriptions)
			for i := 0; i < pthread; i++ {
				wg.Add(1)
				go func(t string) {
					defer wg.Done()
					fmt.Printf("produce %s\n", t)
					for j := 0; j < number; j++ {
						publishMessage(t, fmt.Sprintf("message for %s %d", t, j))
					}
				}(topic)
			}

			for _, sub := range subscriptions {
				for i := 0; i < cthread; i++ {
					wg.Add(1)
					go func(t, q string) {
						defer wg.Done()
						fmt.Printf("consume %s %s\n", t, q)
						receive(q, 5, true)
					}(topic, sub.Endpoint)
				}
			}
		}
	} else {
		if subscriptions, ok := subs[topic]; ok {
			for i := 0; i < pthread; i++ {
				wg.Add(1)
				go func(t string) {
					defer wg.Done()
					fmt.Printf("produce %s\n", t)
					for j := 0; j < number; j++ {
						publishMessage(t, fmt.Sprintf("message for %s %d", t, j))
					}
				}(topic)
			}

			for _, sub := range subscriptions {
				for i := 0; i < cthread; i++ {
					wg.Add(1)
					go func(t, q string) {
						defer wg.Done()
						fmt.Printf("consume %s %s\n", t, q)
						receive(q, 15, true)
					}(topic, sub.Endpoint)
				}
			}
		}
	}

	wg.Wait()
}
