package api

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash"
	"math/rand"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	CmqFieldTag = "cmq"
)

type CMQRequestBuilder interface {
	Build(string, string, interface{}) string
	Parse(url.Values, interface{}) error
}

func NewCMQRequestBuilder(opts CmqConfig) CMQRequestBuilder {
	return &cmqRequestBuilder{opts}
}

type cmqRequestBuilder struct {
	opts CmqConfig
}

func (cb *cmqRequestBuilder) fetchCmqTag(req interface{}, builder *signatureBuilder) {
	t := reflect.TypeOf(req)
	v := reflect.ValueOf(req)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		val := v.Field(i)
		tag := field.Tag.Get(CmqFieldTag)
		if tag == "" {
			continue
		}

		switch val.Kind() {
		case reflect.String, reflect.Int:
			tags := strings.Split(tag, ",")
			// if filed zero value and no default value, it will be ingored
			if reflect.DeepEqual(val.Interface(), reflect.Zero(v.Field(i).Type()).Interface()) {
				if len(tags) < 2 {
					continue
				}
				builder.Append(tags[0], tags[1])
			} else {
				builder.Append(tags[0], fmt.Sprintf("%v", val))
			}
		case reflect.Slice:
			prefix, start, _, err := cb.parseSliceTag(tag)
			if err != nil {
				fmt.Println(err)
				break
			}

			for j := 0; j < val.Len(); j++ {
				vitem := val.Index(j)
				builder.Append(fmt.Sprintf("%s.%d", prefix, start+j), fmt.Sprintf("%v", vitem))
			}
		default:
			fmt.Println("not support kind", val.Kind())
		}
	}
}

func (cb *cmqRequestBuilder) Build(target string, action string, req interface{}) string {
	builder := newSignatureBuilder()
	cb.fetchCmqTag(req, builder)
	return builder.Build(target, action, &cb.opts)
}

func (cb *cmqRequestBuilder) Parse(vals url.Values, req interface{}) error {
	t := reflect.TypeOf(req)
	v := reflect.ValueOf(req)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		val := v.Field(i)
		tag := field.Tag.Get(CmqFieldTag)
		if tag == "" {
			continue
		}
		switch val.Kind() {
		case reflect.Int:
			tags := strings.Split(tag, ",")
			values, ok := vals[tags[0]]
			if ok {
				i, err := strconv.Atoi(values[0])
				if err != nil {
					break
				}
				val.SetInt(int64(i))
			} else if len(tags) > 1 {
				i, err := strconv.Atoi(tags[1])
				if err != nil {
					break
				}
				val.SetInt(int64(i))
			}
		case reflect.String:
			tags := strings.Split(tag, ",")
			values, ok := vals[tags[0]]
			if ok {
				val.SetString(values[0])

			} else if len(tags) > 1 {
				val.SetString(tags[1])
			}
		case reflect.Slice:
			prefix, start, end, err := cb.parseSliceTag(tag)
			if err != nil {
				fmt.Println(err)
				break
			}

			len := end - start + 1
			// just support string slice
			if _, ok := vals[fmt.Sprintf("%s.%d", prefix, start)]; ok {
				val.Set(reflect.MakeSlice(reflect.TypeOf([]string{}), len, len))
			} else {
				break
			}

			for j := start; j <= end; j++ {
				tempTag := fmt.Sprintf("%s.%d", prefix, j)
				values, ok := vals[tempTag]
				if !ok {
					break
				}
				vitem := val.Index(j - start)
				vitem.SetString(values[0])
			}
		default:
			return fmt.Errorf("not suport kind %v", val.Kind())
		}
	}
	return nil
}

// cmq:"msgBody.0-15,xxx"
func (cb *cmqRequestBuilder) parseSliceTag(tag string) (prefix string, start, end int, err error) {
	tagSlice := strings.Split(strings.Split(tag, ",")[0], ".")
	if len(tagSlice) != 2 {
		err = fmt.Errorf("invalid slice tag")
		return
	}
	prefix = tagSlice[0]
	indexs := strings.Split(tagSlice[1], "-")
	if len(tagSlice) != 2 {
		err = fmt.Errorf("invalid slice tag")
		return
	}
	start, err = strconv.Atoi(indexs[0])
	if err != nil {
		return
	}
	end, err = strconv.Atoi(indexs[1])
	return
}

type signatureBuilder struct {
	entryt  strings.Builder
	request strings.Builder
	keys    []string
	values  map[string]string
}

func newSignatureBuilder() *signatureBuilder {
	return &signatureBuilder{
		values: make(map[string]string),
	}
}

func (b *signatureBuilder) Append(k, v string) {
	b.keys = append(b.keys, k)
	b.values[k] = v
}

func (b *signatureBuilder) Build(target string, action string, opts *CmqConfig) string {
	var f func() hash.Hash
	if opts.SignatureMethod == "HmacSHA256" {
		f = sha256.New
	} else {
		f = sha1.New
	}

	b.entryt.WriteString("POST")
	if strings.HasPrefix(target, "http://") {
		b.entryt.WriteString(strings.TrimPrefix(target, "http://"))
	} else if strings.HasPrefix(target, "https://") {
		b.entryt.WriteString(strings.TrimPrefix(target, "https://"))
	} else {
		panic("invalid url scheme: " + target)
	}
	b.entryt.WriteString("?")
	// common values
	// Action
	b.entryt.WriteString("Action=" + action)
	b.request.WriteString("Action=" + url.QueryEscape(action))
	// Nonce
	nonce := strconv.FormatInt(rand.Int63(), 10)
	b.entryt.WriteString("&Nonce=" + nonce)
	b.request.WriteString("&Nonce=" + url.QueryEscape(nonce))
	// Region
	b.entryt.WriteString("&Region=" + opts.Region)
	b.request.WriteString("&Region=" + url.QueryEscape(opts.Region))
	// SecretId
	b.entryt.WriteString("&SecretId=" + opts.SecretId)
	b.request.WriteString("&SecretId=" + url.QueryEscape(opts.SecretId))
	// SignatureMethod
	b.entryt.WriteString("&SignatureMethod=" + opts.SignatureMethod)
	b.request.WriteString("&SignatureMethod=" + url.QueryEscape(opts.SignatureMethod))
	// Timestamp
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	b.entryt.WriteString("&Timestamp=" + timestamp)
	b.request.WriteString("&Timestamp=" + url.QueryEscape(timestamp))

	sort.Strings(b.keys)
	for _, k := range b.keys {
		v := b.values[k]
		b.entryt.WriteString("&" + k + "=" + v)
		b.request.WriteString("&" + k + "=" + url.QueryEscape(v))
	}

	h := hmac.New(f, []byte(opts.SecretKey))
	h.Write([]byte(b.entryt.String()))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	b.request.WriteString("&Signature=" + url.QueryEscape(signature))
	// log.Println(b.request.String())
	return b.request.String()
}
