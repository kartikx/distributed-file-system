package main

const (
	SERVER_HOST            = "localhost"
	SERVER_PORT            = 6400
	INTRODUCER_SERVER_HOST = "172.22.156.212"
	// Increased this from 4, because responses to messages such as REPLICATE aren't currently
	// checking for the piggyback messages contained inside. However, these responses are piggybacking
	// messages.
	PIGGYBACK_TTL                  = 8
	TIMEOUT_DETECTION_MILLISECONDS = 1000
	PING_INTERVAL_MILLISECONDS     = 5000
	RING_NUM_POINTS                = 1000
	NUM_REPLICAS                   = 3
	STORAGE_LOCATION               = "/home/sdevata2/"
)
