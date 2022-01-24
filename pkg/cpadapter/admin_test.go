package cpadapter

import (
	"testing"
)

func TestListTopic(t *testing.T) {
	admin, err := NewPulsarAdmin(AdminOptions{URL: "http://127.0.0.1:8080"})
	if err != nil {
		t.Logf(err.Error())
	}
	resp, err := admin.ListTopics()
	if err != nil {
		t.Logf(err.Error())
	}
	t.Log(resp)
}

func TestGetSubscribe(t *testing.T) {
	admin, err := NewPulsarAdmin(AdminOptions{URL: "http://127.0.0.1:8080"})
	if err != nil {
		t.Logf(err.Error())
	}
	resp, err := admin.GetSubscriptions("test001")
	if err != nil {
		t.Logf(err.Error())
	}
	t.Log(resp)
}
func TestCreateSubscribe(t *testing.T) {
	admin, err := NewPulsarAdmin(AdminOptions{URL: "http://127.0.0.1:8080"})
	if err != nil {
		t.Logf(err.Error())
	}
	err = admin.CreateSubscribe("test001", "sub002")
	if err != nil {
		t.Logf(err.Error())
	}
}

func TestDeleteSubscribe(t *testing.T) {
	admin, err := NewPulsarAdmin(AdminOptions{URL: "http://127.0.0.1:8080"})
	if err != nil {
		t.Logf(err.Error())
	}
	err = admin.Unsubscribe("test001", "sub002")
	if err != nil {
		t.Logf(err.Error())
	}
}
