package amqp

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// URI is a parsed AMQP URI, see https://www.rabbitmq.com/docs/uri-spec.
type URI struct {
	TLS      bool
	Host     string
	Port     int
	Username string
	Password string
	Vhost    string
	// Query parameters recognized by ParseURI, zero when absent.
	Heartbeat      time.Duration // heartbeat=<seconds>, -1 when explicitly 0 (disabled)
	FrameMax       uint32        // frame_max
	ChannelMax     uint16        // channel_max
	ConnectionName string        // name or connection_name
	ConnectTimeout time.Duration // connection_timeout=<milliseconds>
	AuthMechanism  string        // auth_mechanism, "PLAIN" or "EXTERNAL"
	InsecureTLS    bool          // verify=verify_none
	ServerName     string        // server_name_indication
}

// ParseURI parses an amqp:// or amqps:// URI. Unset parts get the default
// values: guest/guest on localhost port 5672 (5671 for amqps) and vhost "/".
func ParseURI(s string) (URI, error) {
	u := URI{Host: "localhost", Username: "guest", Password: "guest", Vhost: "/"}
	if s == "" {
		u.Port = 5672
		return u, nil
	}
	pu, err := url.Parse(s)
	if err != nil {
		return u, fmt.Errorf("amqp: invalid URI: %w", err)
	}
	switch strings.ToLower(pu.Scheme) {
	case "amqp":
		u.Port = 5672
	case "amqps":
		u.TLS = true
		u.Port = 5671
	default:
		return u, fmt.Errorf("amqp: invalid URI scheme %q, expected amqp or amqps", pu.Scheme)
	}
	if h := pu.Hostname(); h != "" {
		u.Host = h
	}
	if p := pu.Port(); p != "" {
		if u.Port, err = strconv.Atoi(p); err != nil || u.Port < 1 || u.Port > 65535 {
			return u, fmt.Errorf("amqp: invalid port %q", p)
		}
	}
	if pu.User != nil {
		u.Username = pu.User.Username()
		if p, ok := pu.User.Password(); ok {
			u.Password = p
		}
	}
	if path := pu.EscapedPath(); len(path) > 1 {
		if u.Vhost, err = url.PathUnescape(path[1:]); err != nil {
			return u, fmt.Errorf("amqp: invalid vhost: %w", err)
		}
	}
	q := pu.Query()
	if v := q.Get("heartbeat"); v != "" {
		n, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			return u, fmt.Errorf("amqp: invalid heartbeat %q", v)
		}
		u.Heartbeat = time.Duration(n) * time.Second
		if n == 0 {
			u.Heartbeat = -1
		}
	}
	if v := q.Get("frame_max"); v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return u, fmt.Errorf("amqp: invalid frame_max %q", v)
		}
		u.FrameMax = uint32(n)
	}
	if v := q.Get("channel_max"); v != "" {
		n, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			return u, fmt.Errorf("amqp: invalid channel_max %q", v)
		}
		u.ChannelMax = uint16(n)
	}
	if v := q.Get("connection_timeout"); v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return u, fmt.Errorf("amqp: invalid connection_timeout %q", v)
		}
		u.ConnectTimeout = time.Duration(n) * time.Millisecond
	}
	u.ConnectionName = q.Get("connection_name")
	if v := q.Get("name"); v != "" {
		u.ConnectionName = v
	}
	u.AuthMechanism = strings.ToUpper(q.Get("auth_mechanism"))
	u.InsecureTLS = q.Get("verify") == "verify_none"
	u.ServerName = q.Get("server_name_indication")
	return u, nil
}

// Addr returns the host:port to dial.
func (u URI) Addr() string {
	return net.JoinHostPort(u.Host, strconv.Itoa(u.Port))
}

// String formats the URI without the password.
func (u URI) String() string {
	scheme := "amqp"
	if u.TLS {
		scheme = "amqps"
	}
	pu := url.URL{Scheme: scheme, Host: u.Addr(), User: url.User(u.Username)}
	pu.RawPath = "/" + url.PathEscape(u.Vhost)
	pu.Path = "/" + u.Vhost
	return pu.String()
}
