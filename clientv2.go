package configv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/17media/logrus"
	"github.com/facebookgo/stats"
)

// Client provides a interface for configuration service
type Client interface {
	// ConfigInfo returns the ConfigInfo protobuf struct that
	// contains infomation w.r.t to repo and last commit
	ConfigInfo() ConfigInfo

	// Get request the content of file at path. If the path doesn't
	// exists then ok returns false
	Get(path string) ([]byte, error)

	// AddListener returns a chan that when any file that matches
	// the pathRegEx changes, the ModifiedFile will be sent over
	// through the channel
	AddListener(pathRegEx *regexp.Regexp) *chan ModifiedFile

	// Watch watches change of a file and invokes callback. It also invokes callback for the
	// first time and return error if there's one.
	Watch(path string, callback func([]byte) error, errChan chan<- error) error

	// RemoveListener remove a listener channel
	RemoveListener(ch *chan ModifiedFile)

	// List lists content under certain path
	List(path string) (map[string][]byte, error)

	// Stop is to stop client
	Stop() error
}

// NewClientV2 return a new Client with option funtions.
// conn: a etcd client,
// root: the root path that contains the config tree
func NewClientV2(conn *clientv3.Client, root string, options ...func(*clientImpl) error) (Client, error) {
	client := &clientImpl{
		etcdConn:  conn,
		watcher:   clientv3.NewWatcher(conn),
		root:      root,
		listeners: []*regexChan{},
		cache:     make(map[string][]byte),
		listers:   make(map[string]*lister),
		ctr:       dummyStat{},
	}

	// set option
	for _, opt := range options {
		if err := opt(client); err != nil {
			return nil, err
		}
	}

	// start config client
	err := client.init()
	return client, err
}

// Stat sets counter client
func Stat(counter stats.Client) func(*clientImpl) error {
	return func(c *clientImpl) error {
		if counter == nil {
			return errors.New("nil counter is not allowed")
		}
		c.ctr = counter
		return nil
	}
}

// NoMatchingLogs skip matching logs
func NoMatchingLogs() func(*clientImpl) error {
	return func(c *clientImpl) error {
		c.noMatchingLogs = true
		return nil
	}
}

// regexChan ties up a Regexp with a channel, chan string
type regexChan struct {
	regex *regexp.Regexp
	ch    *chan ModifiedFile
}

// clientImpl implements the Client interface
type clientImpl struct {
	infoLock     sync.Mutex
	info         ConfigInfo
	etcdConn     *clientv3.Client
	watcher      clientv3.Watcher
	root         string
	listenerLock sync.Mutex
	listeners    []*regexChan
	cacheLock    sync.Mutex
	cache        map[string][]byte
	ctr          stats.Client
	// listers is a map from path to all listers.
	listers map[string]*lister
	// listerLock protects listers
	listerLock     sync.Mutex
	noMatchingLogs bool
}

// init start monitoring the config file
func (c *clientImpl) init() error {
	ctx := context.TODO()
	// no need to check if root dir exists for v3

	// start loop monitor
	infoPath := fmt.Sprintf(infoPrefix, c.root)
	logrus.Infof("start to monitor %s", infoPath)
	return c.watchRootInfo(ctx, infoPath)
}

// watchRootInfo starts to listen on configure info stored in storer, and
// starts dispatching to listeners
func (c *clientImpl) watchRootInfo(ctx context.Context, infoPath string) error {
	// TODO: determin if we need revision from get first
	c.ctr.BumpSum("config.watch.event", 1)
	getResp, err := c.etcdConn.Get(ctx, infoPath)
	if err != nil {
		logrus.Errorf("etcd client Get failed for infoPath<%s>, err: %s", infoPath, err)
		return err
	} else if getResp.Count == 0 {
		logrus.Errorf("etcd infoPath<%s> doesn't exist", infoPath)
		return fmt.Errorf("etcd infoPath<%s> doesn't exist", infoPath)
	} else if getResp.Count > 1 {
		// should not happen because we are not using options to get more than one key
		logrus.Errorf("etcd client Get more than 1 key for infoPath<%s>", infoPath)
		return fmt.Errorf("etcd client Get more than 1 key for infoPath<%s>", infoPath)
	}

	_, err = c.configMarshal(getResp.Kvs[0].Value)
	if err != nil {
		logrus.Infof("get etcd root config is wrong, err: %s", err)
		return err
	}

	// watch info path
	logrus.Infof("starting watching %s", infoPath)
	watchRespChan := c.etcdConn.Watch(ctx, infoPath)

	go func() {
		for watchResp := range watchRespChan {
			if len(watchResp.Events) != 1 {
				logrus.Infof("watch response events length not 1")
				continue
			}
			fmt.Println(watchResp)
			// unmarshal the config
			info, err := c.configMarshal(watchResp.Events[0].Kv.Value)
			if err != nil {
				logrus.Infof("watch etcd root config is wrong, err: %s", err)
				continue
			}
			// empty cache
			for _, f := range info.ModFiles {
				if _, ok := c.cache[f.Path]; !ok {
					continue
				}
				logrus.Infof("delete %v from cache", f.Path)
				c.cacheLock.Lock()
				delete(c.cache, f.Path)
				c.cacheLock.Unlock()
			}

			// send file change event to listeners
			logrus.Infof("fire file change event on info: %v", info)
			c.fireFileChangeEvent(&info)
		}
	}()
	return nil
}
func (c *clientImpl) closeWatcher() error {
	// when close watcher it sometimes returns context.Caneled and that is ok
	if err := c.watcher.Close(); err != nil && err != context.Canceled {
		logrus.Infof("watcher.Close failed, err: %s", err)
		return err
	}

	return nil
}

// configMarshal returns ConfigInfo
func (c *clientImpl) configMarshal(b []byte) (ConfigInfo, error) {
	info := ConfigInfo{}
	err := json.Unmarshal(b, &info)
	if err != nil {
		// the content of node has problem
		c.ctr.BumpSum(cRootDataErr, 1)
		logrus.Errorf("can't unmarshal data: %v, error: %v", b, err)
		return info, err
	}
	return info, nil
}

// ConfigInfo returns the ConfigInfo protobuf struct that
// contains infomation w.r.t to repo and last commit
// NOTE: only set after watch event
func (c *clientImpl) ConfigInfo() ConfigInfo {
	c.infoLock.Lock()
	defer c.infoLock.Unlock()
	return c.info
}

// Get request the content of file at path. If the path doesn't
// exists then ok returns false
func (c *clientImpl) Get(path string) ([]byte, error) {
	defer c.ctr.BumpTime(cGetProcTime).End()
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()
	ctx := context.TODO()
	// check cache
	if bytes, ok := c.cache[path]; ok {
		// cache hit
		return bytes, nil
	}

	c.ctr.BumpSum(cCacheMiss, 1)
	resp, err := c.etcdConn.Get(ctx, filepath.Join(c.root, path))
	if err != nil {
		// get from etcd error
		return nil, err
	}
	if len(resp.Kvs) != 1 {
		return nil, fmt.Errorf("response kvs not 1")
	}
	c.cache[path] = []byte(resp.Kvs[0].Value)
	return c.cache[path], nil
}

// List list the path tree under give path
// TODO: this is legacy method not used in goapi config service, need to check implementation
func (c *clientImpl) List(path string) (map[string][]byte, error) {
	path = strings.Trim(path, "/")
	logrus.Infof("List trimmed path %s", path)
	c.listerLock.Lock()
	defer c.listerLock.Unlock()
	// if we alreay listed this path, just return it
	if l, ok := c.listers[path]; ok {
		return l.List(), nil
	}
	// otherwise, make a new lister and return.
	l, err := newLister(c, path)
	if err != nil {
		return nil, err
	}
	c.listers[path] = l
	return l.List(), nil
}

// AddListener returns a chan that when any file that matches
// the pathRegEx changes, the changed file will be sent over
// through the channel
// the listener only allow to lieten on the files that under root
// directory
func (c *clientImpl) AddListener(pathRegEx *regexp.Regexp) *chan ModifiedFile {
	c.listenerLock.Lock()
	defer c.listenerLock.Unlock()

	cn := make(chan ModifiedFile)
	c.listeners = append(c.listeners, &regexChan{regex: pathRegEx, ch: &cn})
	return &cn
}

func (c *clientImpl) Watch(
	path string,
	callback func([]byte) error,
	errChan chan<- error,
) error {
	// do is a helper function that gets a config path and invokes callback
	do := func() error {
		data, err := c.Get(path)
		if err != nil {
			return err
		}
		return callback(data)
	}

	// Invoke callback for initialization
	if err := do(); err != nil {
		return err
	}

	// Listen to specified path and invoke callback accordingly
	ch := c.AddListener(regexp.MustCompile(path))
	go func() {
		for range *ch {
			if err := do(); err != nil {
				if errChan != nil {
					errChan <- err
				}
			}
		}
	}()
	return nil
}

// RemoveListener remove a listener channel. RemoveListener will not call
// close on ch. However, after RemoveListener call, ch will no longer receive
// events
func (c *clientImpl) RemoveListener(ch *chan ModifiedFile) {
	c.listenerLock.Lock()
	defer c.listenerLock.Unlock()

	for i, regch := range c.listeners {
		if regch.ch == ch {
			c.listeners = append(c.listeners[:i], c.listeners[i+1:]...)
			break
		}
	}
}

// Stop is to stop client
func (c *clientImpl) Stop() error {
	c.closeWatcher()
	// return error is not needed, just to align previous design
	return nil
}

// fireFileChangeEvent is called whenever ConfigInfo.Files changes
func (c *clientImpl) fireFileChangeEvent(info *ConfigInfo) {
	c.infoLock.Lock()
	c.info = *info
	c.infoLock.Unlock()

	// need to clone listener as the callback could call AddListener/RemoveListener
	c.listenerLock.Lock()
	listeners := c.listeners[:]
	c.listenerLock.Unlock()

	for _, regch := range listeners {
		for _, f := range info.ModFiles {
			if !c.noMatchingLogs {
				logrus.Infof("Matching listener <%v> vs file <%v>", regch, f)
			}
			if regch.regex.Match([]byte(f.Path)) {
				logrus.Infof("Matched listener <%v> vs file <%v>", regch, f)
				*regch.ch <- f
				break
			}
		}
	}
}
