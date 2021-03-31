all: setup test clean

setup:
	@echo Creating test dependencies
	fallocate -l 1G /tmp/disk1
	zpool create test_zpool /tmp/disk1

test:
	@echo Running Go tests
	go test -v ./pkg/zfs

clean:
	@echo Cleaning up dependencies
	zpool destroy test_zpool
	rm -f /tmp/disk1
