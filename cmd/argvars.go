package cmd

import "time"

var (
	cfgFile            string        // -config
	delimitString      string        // -delimit
	gcpProject         string        // -project
	rateLimit          float64       // -rate
	timeout            time.Duration // -rate
	maxMessages        int           // -max
	pubsubSubscription string        // -subscription
	pubsubTopic        string        // -topic
	verbose            bool          // -verbose
)
