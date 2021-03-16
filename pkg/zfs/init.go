package zfs

import (
  "bufio"
  "io"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
)

const zfsPath = "/usr/sbin/zfs"
const zpoolPath = "/usr/sbin/zpool"

// zfs module was initialized
// perform pre-flight checks
func init() {

	var err error

	// zfs check
	{
		// does zfs exist?
		_, err = os.ReadFile(zfsPath)
		if err != nil {
			log.Printf("%s not found", zfsPath)
			log.Fatal(err)
		}

		// does zfs work?
		cmd := exec.Command("zfs", "version")
		cmdString := getCommandString(cmd)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Fatal(err)
		}

		stderr, err := cmd.StderrPipe()
		if err != nil {
			log.Fatal(err)
		}

		cmd.Start()
		<-logPipe(stdout, "%s out", cmdString)
		<-logPipe(stderr, "%s err", cmdString)
	}

	// check zpool
	{
		// does zpool exist?
		_, err = os.ReadFile(zpoolPath)
		if err != nil {
			log.Printf("%s not found", zpoolPath)
			log.Fatal(err)
		}

		// does zpool work?
		cmd := exec.Command(zpoolPath, "status", "-x")
		cmdString := getCommandString(cmd)

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Fatal(err)
		}

		stderr, err := cmd.StderrPipe()
		if err != nil {
			log.Fatal(err)
		}
		cmd.Start()
		<-logPipe(stdout, "%s out", cmdString)
		<-logPipe(stderr, "%s err", cmdString)
	}
}

func getCommandString(cmd *exec.Cmd) string {
	basename := path.Base(cmd.Path)
	args := strings.Join(cmd.Args[1:], " ")
	return fmt.Sprintf("%s %s", basename, args)
}

func logPipe(r io.ReadCloser, format string, message ...interface{}) chan bool {
        done := make(chan bool)
        go func() {
                in := bufio.NewScanner(r)
                for in.Scan() {
                        log.Printf("%s: %s", fmt.Sprintf(format, message...), in.Text())
                }
                done <- true
        }()
        return done
}
