package hbaseclient

// Config holds HBase connection configuration
type Config struct {
	// ZookeeperQuorum is the comma-separated list of Zookeeper hosts
	ZookeeperQuorum string
	// ZookeeperPort is the Zookeeper client port (default: 2181)
	ZookeeperPort string
	// ZookeeperRoot is the znode parent in Zookeeper (default: /hbase)
	ZookeeperRoot string
}

// DefaultConfig returns a Config with default values
func DefaultConfig() *Config {
	return &Config{
		ZookeeperQuorum: "hbase",
		ZookeeperPort:   "2181",
		ZookeeperRoot:   "/hbase",
	}
}

// NewConfig creates a new Config with the given Zookeeper quorum
func NewConfig(zkQuorum string) *Config {
	cfg := DefaultConfig()
	cfg.ZookeeperQuorum = zkQuorum
	return cfg
}

// GetZookeeperAddress returns the full Zookeeper address string
func (c *Config) GetZookeeperAddress() string {
	return c.ZookeeperQuorum + ":" + c.ZookeeperPort
}
