package zfs

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"testing"
)

var zpoolName string = "tank"
var z Zpool

func TestMain(m *testing.M) {
	var err error

	z, err = New(zpoolName)
	if err != nil {
		log.Fatalf("zpool %q doesn't exist", zpoolName)
	}

	m.Run()
}

func TestNew(t *testing.T) {

	// bogus case
	{
		name := "bogus"
		_, err := New(name)
		if err == nil {
			t.Errorf("%s zpool shouldn't exist", name)
		}
	}

	// working case
	{
		name := "tank"
		_, err := New(name)
		if err != nil {
			t.Errorf("%s zpool doesn't exist", name)
		}
	}
}

func TestCreateSnapshot(t *testing.T) {

	var err error

	// create a new filesystem
	fs := Filesystem{Name: fmt.Sprintf("%s/new_fs_%s", z.Name, uuid.New())}
	fs, err = z.CreateFilesystem(fs)
	if err != nil {
		t.Errorf("failed to create new filesystem %q", fs.Name)
	} else {
		t.Logf("created new filesystem %s, guid: %s, origin: %s, createtxg: %d\n", fs.Name, fs.GUID, fs.Origin, fs.CreateTxg)
	}

	// create a snapshot on the new filesystem
	snapName := fmt.Sprintf("%s@new_snap_%s", fs.Name, uuid.New())
	snap, err := z.CreateSnapshot(snapName)
	if err != nil {
		t.Errorf("failed to create new snapshot %q", snapName)
	} else {
		t.Logf("created new snapshot %s, guid: %s, createtxg: %d\n", snap.Name, snap.GUID, snap.CreateTxg)
	}

}

func TestCreateFilesystem(t *testing.T) {
	// create a new filesystem
	var snap Snapshot // save for when creating a clone filesystem
	var err error

	{
		fs := Filesystem{Name: fmt.Sprintf("%s/new_fs_%s", z.Name, uuid.New())}
		fs, err = z.CreateFilesystem(fs)
		if err != nil {
			t.Errorf("failed to create new filesystem %q", fs.Name)
		} else {
			t.Logf("created new filesystem %s, guid: %s, origin: %s, createtxg: %d\n", fs.Name, fs.GUID, fs.Origin, fs.CreateTxg)
		}

		// create a snapshot on the new filesystem
		snapName := fmt.Sprintf("%s@new_snap_%s", fs.Name, uuid.New())
		snap, err = z.CreateSnapshot(snapName)
		if err != nil {
			t.Errorf("failed to create new snapshot %q", snapName)
		} else {
			t.Logf("created new snapshot %s, guid: %s, createtxg: %d\n", snap.Name, snap.GUID, snap.CreateTxg)
		}
	}

	// create a new clone filesystem
	{
		fs := Filesystem{Name: fmt.Sprintf("%s/new_clonefs_%s", z.Name, uuid.New()), Origin: snap.Name}
		fs, err = z.CreateFilesystem(fs)
		if err != nil {
			t.Errorf("failed to create new clone filesystem %q on using origin %q", fs.Name, fs.Origin)
		} else {
			t.Logf("created new clone filesystem %s, guid: %s, origin: %s, createtxg: %d\n", fs.Name, fs.GUID, fs.Origin, fs.CreateTxg)
		}
	}
}

func TestExistsByName(t *testing.T) {

	// get all filesystems
	l, err := z.ListFilesystems()
	if err != nil {
		t.Errorf("unable to get filesystems on %s, received %+v", z.Name, err)
	}

	// working name case
	for _, ds := range l {
		if exists := z.ExistsByName(ds.Name); !exists {
			t.Errorf("dataset %q doesn't exist", ds.Name)
		}
	}

	// bogus name case
	{
		name := "bogus/bogus"
		if exists := z.ExistsByName(name); exists {
			t.Errorf("dataset %q should not exist", name)
		}
	}

	// empty name case
	{
		name := ""
		if exists := z.ExistsByName(name); exists {
			t.Errorf("dataset %q should not exist", name)
		}
	}

}

func TestExistsByGUID(t *testing.T) {

	// get zpool filesystem
	fs, err := z.GetFilesystem(zpoolName)
	if err != nil {
		t.Errorf("unable to get filesystem %s", zpoolName)
	}

	// check guid of zpool filesystem
	{
		guid := fs.GUID
		if exists := z.ExistsByGUID(guid); !exists {
			t.Errorf("filesystem %q with guid %q doesn't exist", fs.Name, guid)
		}
	}

	// bogus guid case
	{
		guid := "bogus"
		if exists := z.ExistsByGUID(guid); exists {
			t.Errorf("guid %q should not exist", guid)
		}
	}

	// empty guid case
	{
		guid := ""
		if exists := z.ExistsByGUID(guid); exists {
			t.Errorf("guid %q should not exist", guid)
		}
	}

}

func TestListFilesystems(t *testing.T) {

	// get all filesystems
	l, err := z.ListFilesystems()
	if err != nil {
		t.Errorf("unable to get filesystems on %s, received %+v", z.Name, err)
	} else {
		// scan over filesystem
		for _, ds := range l {
			t.Logf("found filesystem %s, guid: %s, origin: %s, createtxg: %d\n", ds.Name, ds.GUID, ds.Origin, ds.CreateTxg)
		}
	}
}

func TestListSnapshots(t *testing.T) {

	// get all snapshots
	l, err := z.ListSnapshots()
	if err != nil {
		t.Errorf("unable to get snapshots on %s, received %+v", z.Name, err)
	} else {
		// scan over snapshots
		for _, ds := range l {
			t.Logf("found snapshot %s, guid: %s, createtxg: %d\n", ds.Name, ds.GUID, ds.CreateTxg)
		}
	}
}

func TestClonesOf(t *testing.T) {

	var err error

	// 1. create a new filesystem
	// 2. create a snapshot the new filesystem
	// 2. create many clones from new snapshot
	// 3. retrieve clones of new snapshot

	// create a new filesystem
	fs := Filesystem{Name: fmt.Sprintf("%s/new_fs_%s", z.Name, uuid.New())}
	fs, err = z.CreateFilesystem(fs)
	if err != nil {
		t.Errorf("failed to create new filesystem %q", fs.Name)
	} else {
		t.Logf("created new filesystem %s, guid: %s, origin: %s, createtxg: %d\n", fs.Name, fs.GUID, fs.Origin, fs.CreateTxg)
	}

	// create a snapshot on the new filesystem
	snapName := fmt.Sprintf("%s@new_snap_%s", fs.Name, uuid.New())
	snap, err := z.CreateSnapshot(snapName)
	if err != nil {
		t.Errorf("failed to create new snapshot %q", snapName)
	} else {
		t.Logf("created new snapshot %s, guid: %s, createtxg: %d\n", snap.Name, snap.GUID, snap.CreateTxg)
	}

	// create 10 clones from new snapshot
	count := 10
	for i := 0; i < count; i++ {
		clone := Filesystem{Name: fmt.Sprintf("%s/new_clonefs_%s", z.Name, uuid.New()), Origin: snap.Name}
		clone, err = z.CreateFilesystem(clone)
		if err != nil {
			t.Errorf("failed to create new clone filesystem %q using origin %q", clone.Name, snap.Name)
		}
	}
	t.Logf("created %d clone filesystems from %q", count, snap.Name)

	// retrieve clones of new snapshot
	l, err := z.ClonesOf(snap)
	if err != nil {
		t.Errorf("unable to get clones of %q", snap.Name)
	} else {

		// scan over clones
		for _, c := range l {
			t.Logf("found clone filesystem %q, guid: %q, origin: %q createtxg: %d\n", c.Name, c.GUID, c.Origin, c.CreateTxg)
		}
	}
}

func TestSnapshotsOf(t *testing.T) {

	var err error

	// 1. create a new filesystem
	// 2. create many snapshots on new filesystem
	// 3. retrieve snapshots on new filesystem

	// create a new filesystem
	fs := Filesystem{Name: fmt.Sprintf("%s/new_fs_%s", z.Name, uuid.New())}
	fs, err = z.CreateFilesystem(fs)
	if err != nil {
		t.Errorf("failed to create new filesystem %q", fs.Name)
	} else {
		t.Logf("created new filesystem %s, guid: %s, origin: %s, createtxg: %d\n", fs.Name, fs.GUID, fs.Origin, fs.CreateTxg)
	}

	// create 10 snapshots on new filesystem
	count := 10
	for i := 0; i < count; i++ {
		snapName := fmt.Sprintf("%s@new_snap_%s", fs.Name, uuid.New())
		_, err = z.CreateSnapshot(snapName)
		if err != nil {
			t.Errorf("failed to create new snapshot %q", snapName)
		}
	}
	t.Logf("created %d snapshots on %q", count, fs.Name)

	// retrieve snapshots on new filesystem
	l, err := z.SnapshotsOf(fs)
	if err != nil {
		t.Errorf("unable to get snapshots of %s", fs.Name)
	} else {

		// scan over snapshots
		for _, snap := range l {
			t.Logf("found snapshot %s, guid: %s, createtxg: %d\n", snap.Name, snap.GUID, snap.CreateTxg)
		}
	}
}
