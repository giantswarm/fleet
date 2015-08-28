package udplogger

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	etcd "github.com/coreos/fleet/Godeps/_workspace/src/github.com/coreos/etcd/client"
	"github.com/coreos/fleet/Godeps/_workspace/src/golang.org/x/net/context"

	"github.com/coreos/fleet/log"
)

var logger struct {
	udpConn net.Conn
	addr    string
	enabled bool
	mu      *sync.Mutex
	prefix  string
}

const (
	udpLoggerPath = "/udp-logger/fleet"
)

func init() {
	logger.enabled = false
	logger.mu = new(sync.Mutex)
	logger.prefix, _ = os.Hostname()

	cfg := etcd.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
		Transport: etcd.DefaultTransport,
	}
	c, err := etcd.New(cfg)
	if err != nil {
		log.Info("unable to connect to etcd on localhost", err)
		return
	}
	kapi := etcd.NewKeysAPI(c)

	go func() {
		for _ = range time.Tick(1 * time.Second) {

			resp, err := kapi.Get(context.Background(), udpLoggerPath, nil)
			if err != nil {
				logger.enabled = false
				return
			}
			addr := resp.Node.Value
			if addr != logger.addr {
				if addr == "" {
					log.Info("udplogger disabled")
					logger.addr = addr
					logger.enabled = false
				} else {
					logger.mu.Lock()
					if newConn, err := net.Dial("udp", addr); err == nil {
						logger.enabled = true
						logger.addr = addr
						logger.udpConn = newConn
						log.Info("udplogger dumping to ", addr)
					} else {
						log.Warning("unable to use udplogger address", addr)
					}
					logger.mu.Unlock()
				}
			}
		}
	}()
}

func send(data string) {
	if logger.enabled {
		logger.udpConn.Write([]byte(fmt.Sprintf("%s %s: %s", logger.prefix, time.Now().String(), data)))
	}
}

func Logf(format string, v ...interface{}) {
	send(fmt.Sprintf(format, v...))
}

func Logln(v ...interface{}) {
	send(fmt.Sprintln(v...))
}
