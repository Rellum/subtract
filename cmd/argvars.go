package cmd

var (
	cfgFile            string  // -config
	delimitString      string  // -delimit
	gcpProject         string  // -project
	rateLimit          float64 // -rate
	maxMessages        int     // -max
	pubsubSubscription string  // -subscription
	pubsubTopic        string  // -topic
	verbose            bool    // -verbose
)
