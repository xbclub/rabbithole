package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type globalOpts struct {
	host    string
	port    int
	timeout float64
}

func registerGlobalFlags(fs *flag.FlagSet, g *globalOpts) {
	fs.StringVar(&g.host, "host", g.host, "target host")
	fs.IntVar(&g.port, "port", g.port, "target port")
	fs.Float64Var(&g.timeout, "timeout", g.timeout, "socket timeout seconds")
}

func parsePosInt(s, name string) (int, error) {
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("bad %s %q", name, s)
	}
	return v, nil
}

func parsePosFloat(s, name string) (float64, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("bad %s %q", name, s)
	}
	return v, nil
}

func splitGlobalAndSubcommand(args []string) (globalOpts, string, []string, error) {
	g := globalOpts{host: "127.0.0.1", port: 0, timeout: 15.0}
	i := 0
	for i < len(args) {
		a := args[i]
		if !strings.HasPrefix(a, "-") {
			return g, a, args[i+1:], nil
		}
		name := strings.TrimLeft(a, "-")
		var val string
		hasEq := false
		if eq := strings.IndexByte(name, '='); eq >= 0 {
			val = name[eq+1:]
			name = name[:eq]
			hasEq = true
		}
		consumeVal := func() (string, error) {
			if hasEq {
				return val, nil
			}
			if i+1 >= len(args) {
				return "", fmt.Errorf("missing value for --%s", name)
			}
			i++
			return args[i], nil
		}
		switch name {
		case "host":
			v, err := consumeVal()
			if err != nil {
				return g, "", nil, err
			}
			g.host = v
		case "port":
			v, err := consumeVal()
			if err != nil {
				return g, "", nil, err
			}
			p, err := strconv.Atoi(v)
			if err != nil {
				return g, "", nil, fmt.Errorf("bad --port %q: %w", v, err)
			}
			g.port = p
		case "timeout":
			v, err := consumeVal()
			if err != nil {
				return g, "", nil, err
			}
			t, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return g, "", nil, fmt.Errorf("bad --timeout %q: %w", v, err)
			}
			g.timeout = t
		default:
			return g, "", nil, fmt.Errorf("unknown global flag %q", a)
		}
		i++
	}
	return g, "", nil, nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `usage:
  loadgen sink       <port> <seconds> [maxConns]
  loadgen src        [-R|--reverse] <host:port> <seconds> <conns>
  loadgen revsrc     <port> <seconds> [maxConns]
  loadgen revsink    [-R|--reverse] <host:port> <seconds> <conns>
  loadgen target     --tcp-port P --udp-port P --http-port P [--udp-workers 4] [--ready-file path]
  loadgen [--host H --port P --timeout S] latency    [--count 2000 --warmup 50 --newconn]
  loadgen [--host H --port P --timeout S] connect    [--connections 2000 --parallel 32]
  loadgen [--host H --port P --timeout S] udp        [--sources 1 --size 1200 --duration 10 --window 32 --drain 5 --rto 2]
  loadgen [--host H --port P --timeout S] http       [--connections 16 --duration 10 --path / --host-header H]
  loadgen [--host H --port P --timeout S] concurrent [--connections 256 --hold 5]`)
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}
	g, sub, rest, err := splitGlobalAndSubcommand(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if sub == "" {
		printUsage()
		os.Exit(2)
	}

	var rc int
	switch sub {
	case "sink", "revsrc":
		rc = cmdSink(rest)
	case "src", "revsink":
		rc = cmdSrc(rest)
	case "target":
		rc = cmdTarget(rest)
	case "latency":
		rc = cmdLatency(g, rest)
	case "connect":
		rc = cmdConnect(g, rest)
	case "udp":
		rc = cmdUDP(g, rest)
	case "http":
		rc = cmdHTTP(g, rest)
	case "concurrent":
		rc = cmdConcurrent(g, rest)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", sub)
		printUsage()
		os.Exit(2)
	}
	os.Exit(rc)
}
