// Copyright (c) 2018, Postgres Professional

package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/dbus"
	"github.com/davecgh/go-spew/spew"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3"

	cmdcommon "postgrespro.ru/shardman/cmd"
	"postgrespro.ru/shardman/internal/cluster"
	"postgrespro.ru/shardman/internal/ladle"
	"postgrespro.ru/shardman/internal/shmnlog"
	"postgrespro.ru/shardman/internal/utils"
)

// Here we will store args
var cfg cluster.ClusterStoreConnInfo
var logLevel string

var hostname string
var keeperUnitRegexp *regexp.Regexp
var ourUnitsRegexp *regexp.Regexp

var hl *shmnlog.Logger

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "shardman-bowl",
	Version: cmdcommon.ShardmanVersion,
	Short:   "deployment daemon for shardman",
	PersistentPreRun: func(c *cobra.Command, args []string) {
		hl = shmnlog.GetLoggerWithLevel(logLevel)

		if err := cmdcommon.CheckConfig(&cfg); err != nil {
			hl.Fatalf("%v", err)
		}
	},
	Run: bowlMain,
}

// Entry point
func Execute() {
	if err := utils.SetFlagsFromEnv(rootCmd.PersistentFlags(), "HPBOWL"); err != nil {
		hl.Fatalf("%v", err)
	}

	if err := rootCmd.Execute(); err != nil {
		hl.Fatalf("%v", err)
	}
}

// Executed on package init
func init() {
	cmdcommon.AddCommonFlags(rootCmd, &cfg, &logLevel)
}

type bowlState struct {
	// triggers retry if previous attempt failed
	retryTimer *time.Timer
	ls         *ladle.LadleStore
	watchCh    <-chan clientv3.WatchResponse
	dbusConn   *dbus.Conn
}

const retryStoreConnInterval = 2 * time.Second

func bowlMain(c *cobra.Command, args []string) {
	var b = &bowlState{
		retryTimer: time.NewTimer(0),
	}

	var err error
	b.dbusConn, err = dbus.NewUserConnection()
	if err != nil {
		hl.Fatalf("failed to establish dbus connection: %v", err)
	}
	defer b.dbusConn.Close()

	// some globals
	keeperUnitRegexp = regexp.MustCompile(`^shardman-keeper-` + cfg.ClusterName + `@(.*)-(\d+).service$`)
	ourUnitsRegexp = regexp.MustCompile(`^(shardman-keeper-` + cfg.ClusterName +
		`|shardman-sentinel-` + cfg.ClusterName + `|shardman-proxy-` + cfg.ClusterName +
		`|shardman-monitor-` + cfg.ClusterName + `)`)
	hostname, err = os.Hostname()
	if err != nil {
		hl.Fatalf("failed to get hostname: %v", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	ctx := context.Background()
	ctx, cancelFunc := context.WithCancel(ctx)
	go sigHandler(sigs, cancelFunc)

	// main loop
	for {
		var retryTimerCh <-chan time.Time = nil
		if b.retryTimer != nil {
			retryTimerCh = b.retryTimer.C
		}

		select {
		case <-ctx.Done():
			if b.ls != nil {
				b.ls.Close()
				b.ls = nil
			}
			hl.Infof("stopping bowl")
			return

		case <-retryTimerCh:
			var err error
			// create store, if needed
			if b.ls == nil {
				b.ls, err = ladle.NewLadleStore(&cfg)
				if err != nil {
					b.retryTimer = time.NewTimer(retryStoreConnInterval)
					hl.Errorf("failed to create store: %v", err)
					continue
				}
			}
			if b.watchCh == nil {
				lctx := clientv3.WithRequireLeader(ctx)
				cli := b.ls.Store.GetClient()
				b.watchCh = cli.Watch(lctx, b.ls.LadleDataStorePath())
			}
			bowlReconfigure(ctx, b)

		case wresp, ok := <-b.watchCh:
			if !ok {
				// ctx is canceled; or etcd unrecoverable error,
				// in which case error must have been sent previously
				hl.Debugf("watch chan is closed")
				goto recreateWatch
			}
			if wresp.Canceled {
				// etcd unrecoverable error, hmm
				hl.Errorf("store (watch) failed: %v", wresp.Err())
				goto recreateWatch
			}

			bowlReconfigure(ctx, b)
			continue

		recreateWatch:
			// xxx we are not recreating client here, hoping
			// client has fine retry logic
			b.retryTimer = time.NewTimer(retryStoreConnInterval)
			b.watchCh = nil
		}
	}
}

func sigHandler(sigs chan os.Signal, cancel context.CancelFunc) {
	s := <-sigs
	hl.Debugf("got signal %v", s)
	cancel()
}

type unitI interface {
	getName() string
	disableNow(c *dbus.Conn) error
	restart(c *dbus.Conn) error
	reenableNow(c *dbus.Conn) error
}

// Implements unitI
type unit struct {
	name string
}

func (u unit) getName() string {
	return u.name
}

// stop and disable
func (u unit) disableNow(c *dbus.Conn) error {
	var resCh = make(chan string)

	_, err := c.StopUnit(u.name, "replace", resCh)
	if err != nil {
		hl.Errorf("failed to stop unit %v: %v", u.name, err)
		return err
	}
	res := <-resCh
	if res != "done" && res != "skipped" {
		hl.Errorf("wrong res of stopping unit %v: %v", u.name, res)
		return fmt.Errorf("res is %v", res)
	}

	hl.Debugf("disabling %v", u.name)
	_, err = c.DisableUnitFiles([]string{u.name}, false)
	if err != nil {
		hl.Errorf("failed to disable unit %v: %v", u.name, err)
		return err
	}

	return nil
}

func (u unit) restart(c *dbus.Conn) error {
	var resCh = make(chan string)

	_, err := c.RestartUnit(u.name, "replace", resCh)
	if err != nil {
		hl.Errorf("failed to restart unit %v: %v", u.name, err)
		return err
	}
	res := <-resCh
	if res != "done" && res != "skipped" {
		hl.Errorf("wrong res of restarting unit %v: %v", u.name, res)
		return fmt.Errorf("res is %v", res)
	}
	return nil
}

func (u unit) reenableNow(c *dbus.Conn) error {
	_, _, err := c.EnableUnitFiles([]string{u.name}, false, true)
	if err != nil {
		hl.Errorf("failed to enable unit %v: %v", u.name, err)
		return err
	}
	err = u.restart(c)
	if err != nil {
		return err
	}
	return nil
}

type keeper struct {
	unit
	datadir string
}

func (k keeper) disableNow(c *dbus.Conn) error {
	err := k.unit.disableNow(c)
	if err != nil {
		return err
	}

	hl.Debugf("disabling keeper: purging datadir %s", k.datadir)
	err = os.RemoveAll(k.datadir)
	if err != nil {
		hl.Errorf("failed to delete keeper dir: %v", err)
		return err
	}
	return nil
}

type loadedUnit struct {
	unitI
	unitFileState string
	activeState   string
}

type specUnit struct {
	unitI
	env     map[string]string
	envPath string
}

func formTemplateUnitName(bin string, id string) string {
	return fmt.Sprintf("shardman-%s-%s@%s.service", bin, cfg.ClusterName, id)
}

func formMonitorUnitName() string {
	return fmt.Sprintf("shardman-monitor-%s.service", cfg.ClusterName)
}

func formKeeperDataDir(keeperId ladle.KeeperId) string {
	return fmt.Sprintf("keeper-%s-%s-%d", cfg.ClusterName, keeperId.RepGroup, keeperId.Uid)
}

func formEnvFilename(bin string, id string) string {
	if id != "" {
		id = "-" + id
	}
	return fmt.Sprintf("%s-%s%s.env", bin, cfg.ClusterName, id)
}

// loaded units are ones either enabled or running
func getLoadedUnits(ld *ladle.LadleData, dbusConn *dbus.Conn) ([]loadedUnit, error) {
	var loadedUnits []loadedUnit

	unitStatuses, err := dbusConn.ListUnits()
	if err != nil {
		return nil, err
	}
	for _, s := range unitStatuses {
		if !ourUnitsRegexp.MatchString(s.Name) {
			continue
		}

		hl.Debugf("found our service %v, activestate %v", s.Name, s.ActiveState)
		prop, err := dbusConn.GetUnitProperty(s.Name, "UnitFileState")
		if err != nil {
			return nil, err
		}
		unitFileState := prop.Value.String()

		unit := unit{name: s.Name}
		var uI unitI = unit
		if strings.HasPrefix(s.Name, "shardman-keeper") {
			var datadir string
			matches := keeperUnitRegexp.FindStringSubmatch(s.Name)
			if len(matches) == 3 && ld != nil {
				rg := matches[1]
				uid, _ := strconv.Atoi(matches[2])
				datadir = filepath.Join(ld.Spec.DataDir, formKeeperDataDir(ladle.KeeperId{RepGroup: rg, Uid: uid}))
			}
			uI = keeper{unit: unit, datadir: datadir}
		}
		lu := loadedUnit{unitI: uI, unitFileState: unitFileState, activeState: s.ActiveState}
		loadedUnits = append(loadedUnits, lu)
	}

	return loadedUnits, nil
}

func addStoreOpts(env map[string]string, prefix string, c *cluster.StoreConnInfo, clusterName string) {
	env[prefix+"STORE_BACKEND"] = "etcdv3"
	addStoreOptsNoBackend(env, prefix, c, clusterName)
}

func addStoreOptsNoBackend(env map[string]string, prefix string, c *cluster.StoreConnInfo, clusterName string) {
	env[prefix+"CLUSTER_NAME"] = clusterName
	if c.Endpoints != "" {
		env[prefix+"STORE_ENDPOINTS"] = c.Endpoints
	}
	if c.CAFile != "" {
		env[prefix+"STORE_CA_FILE"] = c.CAFile
	}
	if c.CertFile != "" {
		env[prefix+"STORE_CERT_FILE"] = c.CertFile
	}
	if c.Key != "" {
		env[prefix+"STORE_KEY"] = c.Key
	}

}

func getSpecUnits(ld *ladle.LadleData, l *ladle.NodeLayout, cd *cluster.ClusterData) []specUnit {
	var specUnits = []specUnit{}
	// store is ok, but there is no data for our cluster: shut down (and purge) everything
	if l == nil {
		return specUnits
	}

	for _, k := range l.Keepers {
		{
			id := fmt.Sprintf("%s-%d", k.Id.RepGroup, k.Id.Uid)
			unitName := formTemplateUnitName("keeper", id)
			unit := unit{name: unitName}
			datadirName := formKeeperDataDir(k.Id)
			datadir := filepath.Join(ld.Spec.DataDir, datadirName)
			keeperUnit := keeper{unit: unit, datadir: datadir}

			envFilename := formEnvFilename("keeper", id)
			envPath := filepath.Join(ld.Spec.DataDir, envFilename)
			env := make(map[string]string)

			prefix := "STKEEPER_"
			addStoreOpts(env, prefix, ld.Spec.StoreConnInfo, k.Id.RepGroup)
			env[prefix+"PG_BIN_PATH"] = ld.Spec.PgBinPath
			env[prefix+"DATA_DIR"] = datadir
			// xxx allow listening on something else --
			// needs to be configured for each node.
			// Note that 0.0.0.0 (or *) here won't do, since stolon
			// plainly copies this value in reports to store.
			env[prefix+"PG_LISTEN_ADDRESS"] = hostname
			env[prefix+"PG_PORT"] = strconv.Itoa(k.PgPort)

			env[prefix+"PG_REPL_AUTH_METHOD"] = cd.Spec.PgReplAuthMethod
			// xxx allow in file
			if cd.Spec.PgReplPassword != "" {
				env[prefix+"PG_REPL_PASSWORD"] = cd.Spec.PgReplPassword
			}
			env[prefix+"PG_REPL_USERNAME"] = cd.Spec.PgReplUsername

			env[prefix+"PG_SU_AUTH_METHOD"] = cd.Spec.PgSuAuthMethod
			// xxx allow in file
			if cd.Spec.PgSuPassword != "" {
				env[prefix+"PG_SU_PASSWORD"] = cd.Spec.PgSuPassword
			}
			env[prefix+"PG_SU_USERNAME"] = cd.Spec.PgSuUsername

			env[prefix+"UID"] = fmt.Sprintf("keeper_%d", k.Id.Uid)

			var priority int
			if k.Preferred {
				priority = 1
			} else {
				priority = 0
			}
			env[prefix+"PRIORITY"] = strconv.Itoa(priority)

			specUnit := specUnit{unitI: keeperUnit, env: env, envPath: envPath}
			specUnits = append(specUnits, specUnit)
		}

		// put a sentinel near each keeper
		{
			id := k.Id.RepGroup
			unitName := formTemplateUnitName("sentinel", id)
			unit := unit{name: unitName}

			envFilename := formEnvFilename("sentinel", id)
			envPath := filepath.Join(ld.Spec.DataDir, envFilename)
			env := make(map[string]string)

			prefix := "STSENTINEL_"
			addStoreOpts(env, prefix, ld.Spec.StoreConnInfo, id)

			specUnit := specUnit{unitI: unit, env: env, envPath: envPath}
			specUnits = append(specUnits, specUnit)
		}
	}

	if l.Proxy != nil {
		id := l.Proxy.RepGroup
		unitName := formTemplateUnitName("proxy", id)
		unit := unit{name: unitName}

		envFilename := formEnvFilename("proxy", id)
		envPath := filepath.Join(ld.Spec.DataDir, envFilename)
		env := make(map[string]string)

		prefix := "STPROXY_"
		addStoreOpts(env, prefix, ld.Spec.StoreConnInfo, id)
		// xxx allow listening on something else --
		// needs to be configured for each node.
		// Note that 0.0.0.0 (or *) here won't do, since stolon
		// plainly copies this value in reports to store.
		env[prefix+"LISTEN_ADDRESS"] = hostname
		env[prefix+"PORT"] = strconv.Itoa(l.Proxy.Port)
		// xxx tcp options

		specUnit := specUnit{unitI: unit, env: env, envPath: envPath}
		specUnits = append(specUnits, specUnit)
	}

	if l.Monitor {
		unitName := formMonitorUnitName()
		unit := unit{name: unitName}

		envFilename := formEnvFilename("monitor", "")
		envPath := filepath.Join(ld.Spec.DataDir, envFilename)
		env := make(map[string]string)
		prefix := "SHMNMONITOR_"
		addStoreOpts(env, prefix, ld.Spec.StoreConnInfo, cfg.ClusterName)
		// xxx deadlock timeout

		specUnit := specUnit{unitI: unit, env: env, envPath: envPath}
		specUnits = append(specUnits, specUnit)
	}

	return specUnits
}

// get this unit's config from env file
func (su specUnit) getConfig() (map[string]string, error) {
	c := make(map[string]string)
	f, err := os.Open(su.envPath)
	if err != nil {
		hl.Warnf("Failed to open env file of running unit (cluster data was removed?): %v", err)
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line[0] == '#' {
			continue
		}
		splits := strings.SplitN(line, "=", 2)
		if len(splits) != 2 {
			continue
		}
		c[splits[0]] = splits[1]
	}
	return c, nil
}

// dump this unit config into env file
func (su specUnit) writeConfig() error {
	f, err := os.OpenFile(su.envPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush() // yeah, it is not flushed on close
	for k, v := range su.env {
		fmt.Fprintf(w, "%s=%s\n", k, v)
	}
	return nil
}

// avoid using reflect...
func mapsEqual(m1 map[string]string, m2 map[string]string) bool {
	for k, v1 := range m1 {
		v2, ok := m2[k]
		if !ok || v2 != v1 {
			return false
		}
	}

	for k, v2 := range m2 {
		v1, ok := m1[k]
		if !ok || v1 != v2 {
			return false
		}
	}

	return true
}

func reconfigure(loadedUnits []loadedUnit, specUnits []specUnit, c *dbus.Conn) error {
	// there are always only a few daemons, so O(n^2) is ok

	// shut down removed units
	for _, lu := range loadedUnits {
		shutdown := true
		for _, su := range specUnits {
			if lu.getName() == su.getName() {
				shutdown = false
				break
			}
		}
		if shutdown {
			err := lu.disableNow(c)
			if err != nil {
				return err
			}
		}
	}

	// now (re)start units as spec says
	for _, su := range specUnits {
		hl.Debugf("ensuring unit %s is running", su.getName())
		// if unit is active and enabled, only restart if config changed;
		// otherwise, re-enable and restart unconditionally
		considerOk := false
		for _, lu := range loadedUnits {
			if su.getName() == lu.getName() && lu.unitFileState == "\"enabled\"" && lu.activeState == "active" {
				considerOk = true
				break
			}
		}

		if considerOk {
			hl.Debugf("checking config equality")
			currCfg, err := su.getConfig()
			if err != nil || !mapsEqual(currCfg, su.env) {
				err = su.writeConfig()
				if err != nil {
					hl.Errorf("failed to write config: %v", err)
					return err
				}
				err = su.restart(c)
				if err != nil {
					hl.Errorf("failed to restart unit: %v")
					return err
				}
			}
			hl.Debugf("unit is already ok")
		} else {
			hl.Debugf("(re)enabling unit %v", spew.Sdump(su))
			err := su.writeConfig()
			if err != nil {
				hl.Errorf("failed to write config: %v", err)
				return err
			}
			err = su.reenableNow(c)
			if err != nil {
				hl.Errorf("failed to reenable unit: %v", err)
				return err
			}
		}
	}
	return nil
}

// do the deed
func bowlReconfigure(ctx context.Context, b *bowlState) {
	var err error
	var loadedUnits []loadedUnit
	var specUnits []specUnit
	var l *ladle.NodeLayout = nil

	ld, _, cd, _, err := b.ls.GetLadleAndClusterData(ctx)
	if err != nil {
		hl.Errorf("%v", err)
		goto retry
	}
	if ld != nil && cd == nil {
		hl.Errorf("ladledata exists, but clusterdata not")
		goto retry
	}
	if ld != nil {
		// might be nil
		l = ld.Layout[hostname]
	}
	hl.Debugf("ld is %v, l %v", spew.Sdump(ld), spew.Sdump(l))

	loadedUnits, err = getLoadedUnits(ld, b.dbusConn)
	if err != nil {
		hl.Errorf("%v", err)
		goto retry
	}

	specUnits = getSpecUnits(ld, l, cd)

	hl.Debugf("reconfiguring")
	err = reconfigure(loadedUnits, specUnits, b.dbusConn)
	if err != nil {
		goto retry
	}

	// defuse timer
	b.retryTimer = nil
	return

retry:
	b.retryTimer = time.NewTimer(retryStoreConnInterval)
}
