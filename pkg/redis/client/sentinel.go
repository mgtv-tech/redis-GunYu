package client

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/mgtv-tech/redis-GunYu/config"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/common"
	"github.com/mgtv-tech/redis-GunYu/pkg/redis/client/conn"
)

type SentinelTopology struct {
	SentinelAddress string
	MasterName      string
	Master          config.RedisNode
	Replicas        []config.RedisNode
}

func ResolveSentinel(cfg config.RedisConfig) (*SentinelTopology, error) {
	options := cfg.SentinelOptions
	if options == nil || strings.TrimSpace(options.MasterName) == "" {
		return nil, fmt.Errorf("sentinelOptions.masterName is empty")
	}

	addresses := cfg.SentinelDiscoveryAddresses()
	if len(addresses) == 0 {
		return nil, fmt.Errorf("no sentinel address")
	}

	var errs []error
	for _, sentinelAddress := range addresses {
		topology, err := resolveSentinelAddress(cfg, sentinelAddress)
		if err == nil {
			return topology, nil
		}
		errs = append(errs, fmt.Errorf("sentinel %s: %w", sentinelAddress, err))
	}
	return nil, fmt.Errorf("resolve sentinel master %q failed: %w", options.MasterName, errors.Join(errs...))
}

func resolveSentinelAddress(cfg config.RedisConfig, sentinelAddress string) (*SentinelTopology, error) {
	options := cfg.SentinelOptions
	sentinelCfg := config.RedisConfig{
		Addresses: config.SliceString{sentinelAddress},
		UserName:  options.UserName,
		Password:  options.Password,
		TlsEnable: options.TlsEnable,
		Type:      config.RedisTypeStandalone,
		Otype:     config.RedisTypeSentinel,
	}
	sentinel, err := conn.NewRedisConn(sentinelCfg)
	if err != nil {
		return nil, err
	}
	defer sentinel.Close()

	masterReply, err := sentinel.Do("SENTINEL", "get-master-addr-by-name", options.MasterName)
	if err != nil {
		return nil, fmt.Errorf("get master: %w", err)
	}
	masterAddress, err := parseSentinelMaster(masterReply)
	if err != nil {
		return nil, fmt.Errorf("parse master reply: %w", err)
	}

	replicasReply, err := sentinel.Do("SENTINEL", "replicas", options.MasterName)
	if err != nil {
		return nil, fmt.Errorf("get replicas: %w", err)
	}
	replicaAddresses, err := parseSentinelReplicas(replicasReply)
	if err != nil {
		return nil, fmt.Errorf("parse replicas reply: %w", err)
	}

	master, err := validateSentinelDataNode(cfg, sentinelAddress, masterAddress, config.RedisRoleMaster)
	if err != nil {
		return nil, err
	}

	topology := &SentinelTopology{
		SentinelAddress: sentinelAddress,
		MasterName:      options.MasterName,
		Master:          master,
	}
	for _, replicaAddress := range replicaAddresses {
		replica, err := validateSentinelDataNode(cfg, sentinelAddress, replicaAddress, config.RedisRoleSlave)
		if err != nil {
			continue
		}
		topology.Replicas = append(topology.Replicas, replica)
	}
	return topology, nil
}

func validateSentinelDataNode(cfg config.RedisConfig, sentinelAddress, dataAddress string, expected config.RedisRole) (config.RedisNode, error) {
	dataCfg := config.RedisConfig{
		Addresses: config.SliceString{dataAddress},
		UserName:  cfg.UserName,
		Password:  cfg.Password,
		TlsEnable: cfg.TlsEnable,
		Type:      config.RedisTypeStandalone,
		Otype:     config.RedisTypeSentinel,
	}
	data, err := conn.NewRedisConn(dataCfg)
	if err != nil {
		return config.RedisNode{}, fmt.Errorf("sentinel %s returned data node %s: %w", sentinelAddress, dataAddress, err)
	}
	defer data.Close()

	info, err := common.String(data.Do("INFO", "replication"))
	if err != nil {
		return config.RedisNode{}, fmt.Errorf("sentinel %s validate data node %s: %w", sentinelAddress, dataAddress, err)
	}
	role, err := roleFromReplicationInfo(info)
	if err != nil {
		return config.RedisNode{}, fmt.Errorf("sentinel %s validate data node %s: %w", sentinelAddress, dataAddress, err)
	}
	if role != expected {
		return config.RedisNode{}, fmt.Errorf("sentinel %s returned data node %s with role %s, expected %s", sentinelAddress, dataAddress, role.String(), expected.String())
	}

	host, portText, err := net.SplitHostPort(dataAddress)
	if err != nil {
		return config.RedisNode{}, fmt.Errorf("invalid data node address %q: %w", dataAddress, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return config.RedisNode{}, fmt.Errorf("invalid data node port %q: %w", portText, err)
	}
	return config.RedisNode{
		Ip:      host,
		Port:    port,
		Address: dataAddress,
		Role:    role,
		Health:  "online",
	}, nil
}

func parseSentinelMaster(reply interface{}) (string, error) {
	values, err := common.Values(reply, nil)
	if err != nil {
		return "", err
	}
	if len(values) != 2 {
		return "", fmt.Errorf("expected host and port, got %d fields", len(values))
	}
	host, err := common.String(values[0], nil)
	if err != nil {
		return "", err
	}
	port, err := common.String(values[1], nil)
	if err != nil {
		return "", err
	}
	return joinSentinelAddress(host, port)
}

func parseSentinelReplicas(reply interface{}) ([]string, error) {
	replicas, err := common.Values(reply, nil)
	if err != nil {
		return nil, err
	}
	addresses := make([]string, 0, len(replicas))
	for _, rawReplica := range replicas {
		fields, err := common.Values(rawReplica, nil)
		if err != nil {
			return nil, err
		}
		if len(fields)%2 != 0 {
			return nil, fmt.Errorf("replica has odd field count %d", len(fields))
		}
		values := make(map[string]string, len(fields)/2)
		for i := 0; i < len(fields); i += 2 {
			key, err := common.String(fields[i], nil)
			if err != nil {
				return nil, err
			}
			value, err := common.String(fields[i+1], nil)
			if err != nil {
				return nil, err
			}
			values[strings.ToLower(key)] = value
		}
		if sentinelReplicaUnavailable(values["flags"]) {
			continue
		}
		address, err := joinSentinelAddress(values["ip"], values["port"])
		if err != nil {
			return nil, err
		}
		addresses = append(addresses, address)
	}
	return addresses, nil
}

func sentinelReplicaUnavailable(flags string) bool {
	for _, flag := range strings.Split(strings.ToLower(flags), ",") {
		switch strings.TrimSpace(flag) {
		case "s_down", "o_down", "disconnected", "master_down":
			return true
		}
	}
	return false
}

func joinSentinelAddress(host, port string) (string, error) {
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	if host == "" || port == "" {
		return "", fmt.Errorf("empty sentinel host or port")
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return "", fmt.Errorf("invalid sentinel port %q: %w", port, err)
	}
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	return net.JoinHostPort(host, port), nil
}

func roleFromReplicationInfo(info string) (config.RedisRole, error) {
	for _, line := range strings.Split(info, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok || key != "role" {
			continue
		}
		switch strings.TrimSpace(value) {
		case config.RedisRoleMasterStr:
			return config.RedisRoleMaster, nil
		case config.RedisRoleSlaveStr:
			return config.RedisRoleSlave, nil
		default:
			return config.RedisRoleAll, fmt.Errorf("invalid replication role %q", value)
		}
	}
	return config.RedisRoleAll, fmt.Errorf("missing replication role")
}
