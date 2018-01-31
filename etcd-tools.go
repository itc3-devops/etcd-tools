package etcdTools

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/pkg/transport"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var requestTimeout = 10 * time.Second
var key string
var value string
var err error
var lease string
var LeaseHex clientv3.LeaseID
var cli *clientv3.Client
var dialTimeout = 5 * time.Second

// Endpoints : Define ETCD Endpoints
var Endpoints []string

// EtcdApi : Define ETCD Api version
var EtcdApi string

// tlsInfo : Define TLS connection for ETCD
var tlsInfo transport.TLSInfo

// GetEndpointsConfig : Return ETCD Endpoints
func GetEndpointsConfig() []string {
	os.Setenv("ETCDCTL_API", viper.GetString("Etcd.Api"))
	fmt.Println("ETCD Api version: ", os.Getenv("ETCDCTL_API"))
	os.Setenv("ETCDCTL_ENDPOINTS", viper.GetString("Etcd.Endpoints"))
	fmt.Println("ETCD ENDPOINTS: ", os.Getenv("ETCDCTL_ENDPOINTS"))
	os.Setenv("ETCDCTL_CERT", viper.GetString("Etcd.Cert"))
	fmt.Println("ETCD CERT: ", os.Getenv("ETCDCTL_CERT"))
	os.Setenv("ETCDCTL_CACERT", viper.GetString("Etcd.CaCert"))
	fmt.Println("ETCD CACERT: ", os.Getenv("ETCDCTL_CACERT"))
	os.Setenv("ETCDCTL_KEY", viper.GetString("Etcd.Key"))
	fmt.Println("ETCD KEY: ", os.Getenv("ETCDCTL_KEY"))
	return []string{(viper.GetViper().GetString("Etcd.Endpoints"))}
}

// GetTlsInfo : Return configured TLS info
func GetTlsInfo() transport.TLSInfo {
	return transport.TLSInfo{
		CertFile:      viper.GetViper().GetString("Etcd.Cert"),
		KeyFile:       viper.GetViper().GetString("Etcd.Key"),
		TrustedCAFile: viper.GetViper().GetString("Etcd.CaCert"),
	}
}

// GetEtcdTlsCli : Return new ETCD TLS client
func GetEtcdTlsCli() (*clientv3.Client, error) {
	Endpoints := GetEndpointsConfig()
	tlsInfo := GetTlsInfo()

	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		//fmt.Println("Error setting TLS config: ", err)
		log.WithFields(log.Fields{"ETCD-Tools": "EtcdTlsDialer"}).Error("Failed to create TLS config: ", err)
		os.Exit(1)
	}
	return clientv3.New(clientv3.Config{
		Endpoints:   Endpoints,
		DialTimeout: dialTimeout,
		TLS:         tlsConfig,
	})
}

// GetEtcdCli : Return new ETCD dialer
func GetEtcdCli() (*clientv3.Client, error) {
	Endpoints := GetEndpointsConfig()
	return clientv3.New(clientv3.Config{
		Endpoints: Endpoints,
	})
}

// GetEtcdClient : Return new ETCD dialer based on config TLS settings
func GetEtcdClient() (*clientv3.Client, error) {
	if viper.IsSet("Etcd.Tls") {
		fmt.Println("Creating new TLS Etcd Dialer")
		return GetEtcdTlsCli()
	}
	fmt.Println("Creating new ETCD Dialer")
	return GetEtcdCli()

}

// EtcdHealthMemberList : Return ETCDv3 Cluster Health
func EtcdHealthMemberList() *clientv3.MemberListResponse {
	cli, err := GetEtcdClient()
	if err != nil {
		log.WithFields(log.Fields{"ETCD-Tools": "EtcdTlsDialer"}).Error("Failed to start new ETCD client: ", err)
		os.Exit(1)
	}
	defer cli.Close()
	resp, err := cli.MemberList(context.Background())
	if err != nil {
		switch err {
		case context.Canceled:
			fmt.Printf("ctx is canceled by another routine: %v\n", err)
		case context.DeadlineExceeded:
			fmt.Printf("ctx is attached with a deadline is exceeded: %v\n", err)
		case rpctypes.ErrEmptyKey:
			fmt.Printf("client-side error: %v\n", err)
		default:
			fmt.Printf("bad cluster Endpoints, which are not etcd servers: %v\n", err)
		}
	}
	//fmt.Println("Got ETCD dialer, getting member list...")

	//fmt.Println("members:", len(resp.Members))
	// Output: members: 3

	return resp
}

// GetEtcdLease : Request lease from ETCD and start keepalive
func GetEtcdLease() clientv3.LeaseID {
	SerialNumber := viper.GetString("SerialNumber")
	cli, err := GetEtcdClient()
	if err != nil {
		log.WithFields(log.Fields{"ETCD-Tools": "EtcdTlsDialer"}).Error("Failed to start new ETCD client: ", err)
		os.Exit(1)
	}
	defer cli.Close()

	log.WithFields(log.Fields{"run": "ETCD lease request"}).Debug("Requesting Lease")

	LeaseResp, leaseErr := cli.Grant(context.TODO(), 10)
	if leaseErr != nil {
		log.WithFields(log.Fields{"vrctl": "ETCD lease request"}).Error("Requesting Lease", leaseErr)
	}
	if leaseErr != nil {
		switch err {
		case context.Canceled:
			fmt.Printf("ctx is canceled by another routine: %v\n", err)
		case context.DeadlineExceeded:
			fmt.Printf("ctx is attached with a deadline is exceeded: %v\n", err)
		case rpctypes.ErrEmptyKey:
			fmt.Printf("client-side error: %v\n", err)
		default:
			fmt.Printf("bad cluster endpoints, which are not etcd servers: %v\n", err)
		}
	}
	lease = fmt.Sprintf("%016x", LeaseResp.ID)
	LeaseHex = LeaseResp.ID
	key := strings.Join([]string{"active-lease", SerialNumber}, "/")
	value := lease
	_, err = cli.Put(context.TODO(), key, value, clientv3.WithLease(LeaseResp.ID))
	if err != nil {
		log.WithFields(log.Fields{"vrctl": "ETCD register"}).Error("Adding hex lease to active-devices key in Etcd", err)
	}

	//fmt.Printf("lease %016x in simple format\n", LeaseResp.ID)

	return LeaseResp.ID

}

func EtcdPutLeaseForever(key string, value string) {
	log.WithFields(log.Fields{"ETCD-Tools": "EtcdPutLeaseForever"}).Info("Getting ETCD Dialer")
	cli, err := GetEtcdClient()
	if err != nil {
		log.WithFields(log.Fields{"ETCD-Tools": "EtcdTlsDialer"}).Error("Failed to start new ETCD client: ", err)
		os.Exit(1)
	}
	defer cli.Close()

	log.WithFields(log.Fields{"ETCD-Tools": "EtcdPutLeaseForever"}).Info("Getting Dialer Key Options")
	opts, id := GetEtcdPutOptions()

	_, err = cli.Put(context.TODO(), key, value, opts...)
	if err != nil {
		log.WithFields(log.Fields{"vrctl": "ETCD register"}).Error("Adding hex lease to active-devices key in Etcd", err)
	}
	if err != nil {
		switch err {
		case context.Canceled:
			fmt.Printf("ctx is canceled by another routine: %v\n", err)
		case context.DeadlineExceeded:
			fmt.Printf("ctx is attached with a deadline is exceeded: %v\n", err)
		case rpctypes.ErrEmptyKey:
			fmt.Printf("client-side error: %v\n", err)
		default:
			fmt.Printf("bad cluster endpoints, which are not etcd servers: %v\n", err)
		}
	}
	LeaseKeepAliveCommandFunc(id)

}

// LeaseKeepAliveCommandFunc : executes the "lease keep-alive" command.
func LeaseKeepAliveCommandFunc(leaseID clientv3.LeaseID) {
	// Running keepalive as a go routine will let it run in the background for as long as the main program runs
	go func() {
		cli, err := GetEtcdClient()
		if err != nil {
			log.WithFields(log.Fields{"ETCD-Tools": "EtcdTlsDialer"}).Error("Failed to start new ETCD client: ", err)
			os.Exit(1)
		}
		defer cli.Close()

		fmt.Println("Starting Keepalive")

		fmt.Println("LeaseID: ", leaseID)
		id := leaseID
		respc, kerr := cli.KeepAlive(context.TODO(), id)
		if kerr != nil {
			log.WithFields(log.Fields{"vrctl": "ETCD keepalive"}).Error("Starting Keepalive for lease", kerr)
		}
		if kerr != nil {
			switch err {
			case context.Canceled:
				fmt.Printf("ctx is canceled by another routine: %v\n", kerr)
			case context.DeadlineExceeded:
				fmt.Printf("ctx is attached with a deadline is exceeded: %v\n", kerr)
			case rpctypes.ErrEmptyKey:
				fmt.Printf("client-side error: %v\n", kerr)
			default:
				fmt.Printf("bad cluster endpoints, which are not etcd servers: %v\n", kerr)
			}
		}
		for resp := range respc {
			fmt.Println(*resp)
		}
	}()
}

func GetEtcdPutOptions() ([]clientv3.OpOption, clientv3.LeaseID) {
	var newid clientv3.LeaseID
	SerialNumber := viper.GetString("SerialNumber")
	log.WithFields(log.Fields{"ETCD-Tools": "GetEtcdPutOptions"}).Info("Getting ETCD Dialer")
	cli, err := GetEtcdClient()
	if err != nil {
		log.WithFields(log.Fields{"ETCD-Tools": "EtcdTlsDialer"}).Error("Failed to start new ETCD client: ", err)
		os.Exit(1)
	}
	defer cli.Close()

	fmt.Println("Print ETCD Endpoints: ", viper.GetString("ETCDCTL_ENDPOINTS"))
	log.WithFields(log.Fields{"ETCD-Tools": "GetEtcdPutOptions"}).Info("Setting client options")
	opts := []clientv3.OpOption{}
	// Get lease ID from active device key in ETCD, the most accurate source for the LeaseID
	log.WithFields(log.Fields{"ETCD-Tools": "GetEtcdPutOptions"}).Info("Generate LeaseID storage key")
	key1 := strings.Join([]string{"active-lease", SerialNumber}, "/")
	resp := EtcdGetPrefix(key1)
	log.WithFields(log.Fields{"ETCD-Tools": "GetEtcdPutOptions"}).Info("Checking for existing key")
	if len(resp.Kvs) == 0 {
		log.WithFields(log.Fields{"ETCD-Tools": "GetEtcdPutOptions"}).Info("Key returned no existing LeaseID for this device")
		log.WithFields(log.Fields{"ETCD-Tools": "GetEtcdPutOptions"}).Info("Requested new Lease")
		id := GetEtcdLease()
		log.WithFields(log.Fields{"ETCD-Tools": "GetEtcdPutOptions"}).Info("New lease: ", id)

		if id != 0 {
			opts = append(opts, clientv3.WithLease(clientv3.LeaseID(id)))
		}
		return opts, id

	}
	for _, ev := range resp.Kvs {

		value := fmt.Sprintf("%s", ev.Value)
		if value == "" {
			log.WithFields(log.Fields{"ETCD-Tools": "GetEtcdPutOptions"}).Info("Key returned empty value, requesting new lease")
			id := GetEtcdLease()

			if id != 0 {
				opts = append(opts, clientv3.WithLease(clientv3.LeaseID(id)))
			}
			return opts, id
		}
		log.WithFields(log.Fields{"ETCD-Tools": "GetEtcdPutOptions"}).Info("Key returned existing LeaseID for this device. Checking to see if it has expired")
		// leaseInt, err := strconv.ParseInt(value, 64)
		log.WithFields(log.Fields{"vrctl": "ETCD putLeaseForever"}).Debug("Current Lease:  ", value)
		oldid, err := strconv.ParseInt(value, 16, 64)
		if err != nil {
			log.WithFields(log.Fields{"vrctl": "ETCD putLeaseForever"}).Error("Error parsing LeaseID:  ", err)

		}

		if oldid != 0 {
			opts = append(opts, clientv3.WithLease(clientv3.LeaseID(oldid)))
		}

		hexid := clientv3.LeaseID(oldid)
		key := string("etcd::lease::test::" + SerialNumber)
		EtcdPutLeaseTest(key, "34521", opts)
		_, v := EtcdGetKeyValue(key)
		if v == "34521" {
			return opts, hexid
			log.WithFields(log.Fields{"Task scheduler": "RunScheduledTask8"}).Debug("ETCD Lease is still valid")
		}

		log.WithFields(log.Fields{"ETCD-Tools": "GetEtcdPutOptions"}).Info("Lease for this device has expired, requesting new lease")
		newid = GetEtcdLease()
		log.WithFields(log.Fields{"ETCD-Tools": "GetEtcdPutOptions"}).Info("New lease: ", newid)
		if newid != 0 {
			opts = append(opts, clientv3.WithLease(clientv3.LeaseID(newid)))
		}
		return opts, newid
	}

	return opts, newid
}

// EtcdGetKeyValue : Get a specific ETCD Key and its value returned as strings
func EtcdGetKeyValue(key string) (string, string) {
	cli, err := GetEtcdClient()
	if err != nil {
		log.WithFields(log.Fields{"ETCD-Tools": "EtcdTlsDialer"}).Error("Failed to start new ETCD client: ", err)
		os.Exit(1)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		switch err {
		case context.Canceled:
			fmt.Printf("ctx is canceled by another routine: %v\n", err)
		case context.DeadlineExceeded:
			fmt.Printf("ctx is attached with a deadline is exceeded: %v\n", err)
		case rpctypes.ErrEmptyKey:
			fmt.Printf("client-side error: %v\n", err)
		default:
			fmt.Printf("bad cluster endpoints, which are not etcd servers: %v\n", err)
		}
	}
	for _, ev := range resp.Kvs {

		value = fmt.Sprintf("%s", ev.Value)
		key = fmt.Sprintf("%s", ev.Key)
	}
	// Output: foo : bar
	return key, value
}

// EtcdGetPrefix : Get ETCD Prefix returned as an ETCD clientv3 response
func EtcdGetPrefix(key string) *clientv3.GetResponse {
	cli, err := GetEtcdClient()
	if err != nil {
		log.WithFields(log.Fields{"ETCD-Tools": "EtcdTlsDialer"}).Error("Failed to start new ETCD client: ", err)
		os.Exit(1)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := cli.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		switch err {
		case context.Canceled:
			fmt.Printf("ctx is canceled by another routine: %v\n", err)
		case context.DeadlineExceeded:
			fmt.Printf("ctx is attached with a deadline is exceeded: %v\n", err)
		case rpctypes.ErrEmptyKey:
			fmt.Printf("client-side error: %v\n", err)
		default:
			fmt.Printf("bad cluster endpoints, which are not etcd servers: %v\n", err)
		}
	}
	return resp
}

func EtcdPutLeaseTest(key string, value string, opts []clientv3.OpOption) {
	log.WithFields(log.Fields{"ETCD-Tools": "EtcdPutLeaseForever"}).Info("Getting ETCD Dialer")
	cli, err := GetEtcdClient()
	if err != nil {
		log.WithFields(log.Fields{"ETCD-Tools": "EtcdTlsDialer"}).Error("Failed to start new ETCD client: ", err)
		os.Exit(1)
	}
	defer cli.Close()

	_, err = cli.Put(context.TODO(), key, value, opts...)
	if err != nil {
		log.WithFields(log.Fields{"vrctl": "ETCD register"}).Error("Adding hex lease to active-devices key in Etcd", err)
	}
	if err != nil {
		switch err {
		case context.Canceled:
			fmt.Printf("ctx is canceled by another routine: %v\n", err)
		case context.DeadlineExceeded:
			fmt.Printf("ctx is attached with a deadline is exceeded: %v\n", err)
		case rpctypes.ErrEmptyKey:
			fmt.Printf("client-side error: %v\n", err)
		default:
			fmt.Printf("bad cluster endpoints, which are not etcd servers: %v\n", err)
		}
	}

}
