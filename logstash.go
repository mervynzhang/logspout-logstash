package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"strings"
	"time"
	"github.com/gliderlabs/logspout/router"
	"os"
	"io/ioutil"
	"fmt"
	"path"
	"bufio"
	"path/filepath"
	"github.com/fsouza/go-dockerclient"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn          net.Conn
	route         *router.Route
	containerTags map[string][]string
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route:         route,
		conn:          conn,
		containerTags: make(map[string][]string),
	}, nil
}

// Get container tags configured with the environment variable LOGSTASH_TAGS
func GetContainerTags(c *docker.Container, a *LogstashAdapter) []string {
	if tags, ok := a.containerTags[c.ID]; ok {
		return tags
	}

	var tags = []string{}
	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_TAGS=") {
			tags = strings.Split(strings.TrimPrefix(e, "LOGSTASH_TAGS="), ",")
			break
		}
	}

	a.containerTags[c.ID] = tags
	return tags
}


func send_log(a *LogstashAdapter, pre_log_time, cur_time time.Time){
	JSONFormat := time.RFC3339Nano
	rootf := "/var/lib/docker/containers"
	foders, _ := ioutil.ReadDir(rootf)
	for _, subf := range foders {
		if !subf.IsDir(){
			continue
		}
		files, err := ioutil.ReadDir(filepath.Join(rootf,subf.Name()))
		if err != nil {
			continue
		}

		var jlog Jsonlog
		var tags = []string{}
		tags = append(tags, "resentlogs")

		for _, f := range files {

			m, _ := path.Match("*-json.log", f.Name())
			if m {
				fmt.Printf("checking %s ........\n", f.Name()[:12])
				file, err := os.Open(filepath.Join(rootf,subf.Name(),f.Name()))
				if err != nil {
					log.Println(err)
					continue
				}

				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					//fmt.Println(scanner.Text())
					err := json.Unmarshal(scanner.Bytes(), &jlog)
					if err != nil {
						fmt.Printf("Unmarshal error: %s for %s \n", err, string(scanner.Bytes()))
						continue
					}

					jt, _ := time.Parse(JSONFormat, jlog.Time)

					//fmt.Println(jt)
					if pre_log_time.Before(jt) && cur_time.After(jt) {

						di := DockerInfo{
							Name:     f.Name()[:12],
							ID:       f.Name(),
							Image:    "",
							Hostname: "",
						}

						jsonMsg := LogstashMessage{
							Message: jlog.Log,
							Docker:  di,
							Stream:  "json file",
							Logtime: jlog.Time,
							Tags:    tags,
						}

						js, serr := json.Marshal(jsonMsg)
						if serr != nil {
							log.Printf("json.Marshal in send_log error: %s \n ", serr)
							continue
						}
						//fmt.Printf("sending %s to elk...\n",js)
						js = append(js, byte('\n'))

						go func(a *LogstashAdapter, js []byte) {
							fmt.Printf("in file_to_elk, resending logs: %s\n",js)
							if _, werr := a.conn.Write(js); werr != nil {
								log.Printf("file_to_elk error: %s ....... \n", werr)
							}
						}(a,js)
					}

				}

				if err := scanner.Err(); err != nil {
					log.Println(err)
				}
				file.Close()
			}
		}
	}
	fmt.Println("resend log files DONE!")
}


// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {

	fmt.Println("Read pos_file and check logs")

	JSONFormat := time.RFC3339Nano
	pwd, _ := os.Getwd()
	fmt.Println(pwd)
	pos_file := "pos_file"
	dat, rerr := ioutil.ReadFile(pos_file)
	if rerr == nil {
		pre_log_time, perr := time.Parse(JSONFormat, string(dat))
		if perr == nil {
			fmt.Println("parsed old time...")
			fmt.Println(pre_log_time)

			cur_time :=time.Now().UTC()//.Format(JSONFormat)
			if pre_log_time.Before(cur_time) {
				fmt.Println("calling send_log...")
				go send_log(a, pre_log_time, cur_time)
			}

		}
	}

	for m := range logstream {

		dockerInfo := DockerInfo{
			Name:     m.Container.Name[1:],
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
		}

		tags := GetContainerTags(m.Container, a)

		var js []byte
		var data map[string]interface{}

		log_time := m.Time.UTC().Format(JSONFormat)

		// Parse JSON-encoded m.Data
		if err := json.Unmarshal([]byte(m.Data), &data); err != nil {
			// The message is not in JSON, make a new JSON message.
			msg := LogstashMessage{
				Message: m.Data,
				Docker:  dockerInfo,
				Stream:  m.Source,
				Logtime: log_time,
				Tags:    tags,
			}

			if js, err = json.Marshal(msg); err != nil {
				// Log error message and continue parsing next line, if marshalling fails
				log.Println("logstash: could not marshal JSON:", err)
				continue
			}
		} else {
			// The message is already in JSON, add the docker specific fields.
			data["docker"] = dockerInfo
			data["tags"] = tags
			data["stream"] = m.Source
			data["logtime"] = log_time
			// Return the JSON encoding
			if js, err = json.Marshal(data); err != nil {
				// Log error message and continue parsing next line, if marshalling fails
				log.Println("logstash: could not marshal JSON:", err)
				continue
			}
		}

		// To work with tls and tcp transports via json_lines codec
		js = append(js, byte('\n'))

		go func (a *LogstashAdapter,js []byte){
			if _, werr := a.conn.Write(js); werr != nil {
				log.Printf("to_elk error: %s ....... \n", werr)
			}
		}(a, js)

		//write m.Time to pos_file
		go func() {

			werr := ioutil.WriteFile(pos_file, []byte(log_time), 0644)
			if werr != nil {
				fmt.Println(werr)
			}
		}()

	}
}

type DockerInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message string     `json:"message"`
	Stream  string     `json:"stream"`
	Docker  DockerInfo `json:"docker"`
	Logtime string     `json:"Logtime"`
	Tags    []string   `json:"tags"`
}

type Jsonlog struct {
	Log string
	Stream string
	Time string
}
