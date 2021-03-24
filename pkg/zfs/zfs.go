package zfs

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"os/exec"
	"strconv"
	"strings"
)

type Zpool struct {
	Name string
}

type Filesystem struct {
	Name      string `json:"name"`
	GUID      string `json:"guid"`
	Origin    string `json:"origin"`
	CreateTxg int64  `json:"createtxg"`
}

type Snapshot struct {
	Name      string `json:"name"`
	GUID      string `json:"guid"`
	CreateTxg int64  `json:"createtxg"`
}

type Filesystems map[string]*Filesystem
type Snapshots map[string]*Snapshot

// New returns a new Zpool struct
func New(zpool string) (z Zpool, err error) {

	if ok := zpoolExists(zpool); !ok {
		err := errors.New(fmt.Sprintf("zpool %q doesn't exist", zpool))
		return z, err
	}

	return Zpool{ Name: zpool }, nil

}

// zpoolExists checks if given zpool name exists on the system
func zpoolExists(zpool string) bool {
	err := exec.Command(zpoolPath, "get", "-H", "-o", "value", "name", zpool).Run()
	if err != nil {
		return false
	}
	return true
}

// Snapshots will return an map of snapshots on the zpool
func (z Zpool) ListSnapshots() (l Snapshots, err error) {

	// make map
	l = make(Snapshots, 0)

	//  zfs get -t snapshot -Hro name,property,value guid,createtxg tank
	cmd := exec.Command(zfsPath, "get", "-t", "snapshot", "-Hro", "name,property,value", "guid,createtxg", z.Name)

	// execute command
	out, err := cmd.Output()
	if err != nil {
		cmdString := getCommandString(cmd)
		return l, errors.Wrapf(err, "unable to run command %q", cmdString)
	}

	// begin parsing output
	in := bufio.NewScanner(bytes.NewReader(out))
	for in.Scan() {
		var name, property, value string
		fmt.Sscanf(in.Text(), "%s\t%s\t%s", &name, &property, &value)

		// check if name already exists in map
		_, ok := l[name]
		if !ok {
			l[name] = &Snapshot{Name: name}
		}

		// get it now
		ds, _ := l[name]

		switch property {
		case "guid":
			ds.GUID = value
		case "createtxg":
			p, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return l, errors.Wrapf(err, "unable to convert createtxg value %q to int64", value)
			}
			ds.CreateTxg = p
		}
	}
	return l, nil
}

// CreateFilesystem creates a filesystem on the zpool.
func (z *Zpool) CreateFilesystem(fs Filesystem) (Filesystem, error) {

	// short circuit to error if name doesn't start with zpool name
	if len(fs.Name) == 0 || fs.CreateTxg != 0 || strings.HasPrefix(fs.Name, z.Name) == false {
		return fs, errors.Errorf("filesystem %q cannot be created on zpool %q", fs.Name, z.Name)
	}

	// build command
	var cmd *exec.Cmd

	// check if origin is not empty
	// if origin is set then create new filesystem
	// if origin is not set then create a clone of the origin
	if len(fs.Origin) == 0 || fs.Origin == "-" {
		cmd = exec.Command(zfsPath, "create", fs.Name)
	} else {
		cmd = exec.Command(zfsPath, "clone", fs.Origin, fs.Name)
	}

	// run command
	if _, err := cmd.Output(); err != nil {
		// known ways to fail
		// 1. filesystem already exists
		// 2. filesystem's parent path doesn't exist
		// 3. zfs fails
		return fs, errors.Wrapf(err, "unable to create filesystem %q", fs.Name)
	}

	// retrieve the newly created filesystem
	n, err := z.GetFilesystem(fs.Name)
	if err != nil {
		return fs, errors.Wrapf(err, "unable to retrieve filesystem %q after creation", fs.Name)
	}

	return n, nil
}

// CreateSnapshot creates a snapshot on the filesystem.
func (z *Zpool) CreateSnapshot(snapshotName string) (snap Snapshot, err error) {

	// short circuit to error if name doesn't start with zpool name
	if len(snapshotName) == 0 || strings.HasPrefix(snapshotName, z.Name) == false {
		return snap, errors.Errorf("snapshot %q cannot be created on zpool %q", snapshotName, z.Name)
	}

	// build command
	cmd := exec.Command(zfsPath, "snapshot", snapshotName)

	// run command
	if _, err := cmd.Output(); err != nil {
		// known ways to fail
		// 1. snapshot already exists
		// 2. snapshot on non-existing filesystem
		// 3. zfs fails
		return snap, errors.Wrapf(err, "unable to create snapshot %q", snapshotName)
	}

	// retrieve the newly created snapshot
	snap, err = z.GetSnapshot(snapshotName)
	if err != nil {
		return snap, errors.Wrapf(err, "unable to retrieve snapshot %q after creation", snap.Name)
	}

	return snap, nil
}

// Filesystems will return an map of filesystems on the zpool
func (z Zpool) ListFilesystems() (l Filesystems, err error) {

	// make map
	l = make(Filesystems, 0)

	//  zfs get -t filesystem -Hro name,property,value guid,origin,createtxg tank
	cmd := exec.Command(zfsPath, "get", "-t", "filesystem", "-Hro", "name,property,value", "origin,guid,createtxg", z.Name)

	// execute command
	out, err := cmd.Output()
	if err != nil {
		cmdString := getCommandString(cmd)
		return l, errors.Wrapf(err, "unable to run command %q", cmdString)
	}

	// begin parsing output
	in := bufio.NewScanner(bytes.NewReader(out))
	for in.Scan() {
		var name, property, value string
		fmt.Sscanf(in.Text(), "%s\t%s\t%s", &name, &property, &value)

		// check if name already exists in map, if not create it
		_, ok := l[name]
		if !ok {
			l[name] = &Filesystem{Name: name}
		}

		// get it now
		ds, _ := l[name]

		switch property {
		case "origin":
			ds.Origin = value
		case "guid":
			ds.GUID = value
		case "createtxg":
			p, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return l, errors.Wrapf(err, "unable to convert createtxg value %q to int64", value)
			}
			ds.CreateTxg = p
		}
	}
	return l, nil
}

// ClonesOf will return an array of clone filesystem for given snapshot
func (z Zpool) ClonesOf(s Snapshot) (clones []*Filesystem, err error) {
	clones = make([]*Filesystem, 0)

	// get all filesystems
	l, err := z.ListFilesystems()
	if err != nil {
		return clones, err
	}

	for _, fs := range l {
		if fs.Origin == s.Name {
			clones = append(clones, fs)
		}
	}

	return clones, nil
}

// Filesystem...
func (z Zpool) GetFilesystem(name string) (ds Filesystem, err error) {

	// filesystem name should start with zpool name
	if strings.HasPrefix(name, z.Name) == false {
		return ds, errors.Errorf("bad request for filesystem %q on zpool %q", name, z.Name)
	}
	// example command
	// zfs get -t filesystem -Ho property,value name,guid,createtxg,origin tank/now

	// build command
	cmd := exec.Command(zfsPath, "get", "-t", "filesystem", "-Ho", "property,value", "name,guid,createtxg,origin", name)

	// run command
	out, err := cmd.Output()
	if err != nil {
		return ds, errors.Wrapf(err, "filesystem %q not found", name)
	}

	// parse []byte output
	in := bufio.NewScanner(bytes.NewReader(out))
	for in.Scan() {
		var property, value string
		fmt.Sscanf(in.Text(), "%s\t%s", &property, &value)
		switch property {
		case "name":
			ds.Name = value
		case "guid":
			ds.GUID = value
		case "createtxg":
			// parse the createtxg value into int64
			p, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return ds, errors.Wrapf(err, "unable to parse createtxg value %q to int64", value)
			}
			ds.CreateTxg = p
		case "origin":
			ds.Origin = value
		}
	}

	return ds, nil
}

// Snapshot will return the found Snapshot
func (z Zpool) GetSnapshot(name string) (ds Snapshot, err error) {

	// snapshot name should start with zpool name
	if strings.HasPrefix(name, z.Name) == false {
		return ds, errors.Errorf("bad request for snapshot %q on zpool %q", name, z.Name)
	}

	// build command
	cmd := exec.Command(zfsPath, "get", "-t", "snapshot", "-Ho", "property,value", "name,guid,createtxg", name)

	// run command
	out, err := cmd.Output()
	if err != nil {
		return ds, errors.Errorf("snapshot %q not found", name)
	}

	// parse []byte output
	in := bufio.NewScanner(bytes.NewReader(out))
	for in.Scan() {
		var property, value string
		fmt.Sscanf(in.Text(), "%s\t%s", &property, &value)
		switch property {
		case "name":
			ds.Name = value
		case "guid":
			ds.GUID = value
		case "createtxg":
			// parse the createtxg value into int64
			p, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return ds, errors.Wrapf(err, "unable to parse createtxg value %q to int64", value)
			}
			ds.CreateTxg = p
		}
	}

	return ds, err
}

// SnapshotsOf will return an array of snapshots for given filesystem.
// The snapshot array will only be immediate descandant of the given filesystem.
func (z Zpool) SnapshotsOf(fs Filesystem) (snapshots []*Snapshot, err error) {

	snapshots = make([]*Snapshot, 0)

	l, err := z.ListSnapshots()
	if err != nil {
		return snapshots, err
	}

	for _, ds := range l {
		// the filesystem name is before the @ sign
		fsName := strings.Split(ds.Name, "@")[0]
		if fsName == fs.Name {
			snapshots = append(snapshots, ds)
		}
	}

	return snapshots, nil
}

// ExistsByGUID will return true or false if a matching GUID is found on a dataset in the zpool. This executes a zfs command to get all datasets' GUID on the zpool.
func (z Zpool) ExistsByGUID(guid string) bool {
	// short circuit
	if len(guid) == 0 {
		return false
	}

	// zfs get -r -Ho value guid tank
	cmd := exec.Command(zfsPath, "get", "-r", "-Ho", "value", "guid", z.Name)
	out, err := cmd.Output()
	if err != nil {
		return false
	}

	// scan through lines
	in := bufio.NewScanner(bytes.NewReader(out))
	for in.Scan() {
		if in.Text() == guid {
			return true
		}
	}

	// no match found
	return false
}

// ExistsByName will return true or false if the dataset name is found on the zpool.
func (z Zpool) ExistsByName(name string) bool {

	// short circuit to false if name doesn't start with zpool name
	if len(name) == 0 || strings.HasPrefix(name, z.Name) == false {
		return false
	}

	err := exec.Command(zfsPath, "get", "-Ho", "value", "name", name).Run()
	if err != nil {
		return false
	}
	return true
}
