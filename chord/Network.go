package DHT

import (
	"net"
)

func GetLocalAddress() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

    var localaddr string
	for _, tmp := range ifaces {
		if tmp.Flags&net.FlagLoopback == 0 && tmp.Flags&net.FlagUp != 0 {
			addrs, err := tmp.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddr = ip4.String()
						break
					}
				}
			}
		}
	}
	
	if localaddr == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddr
}
