package zfs

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
)

const zfsPath = "/usr/sbin/zfs"
const zpoolPath = "/usr/sbin/zpool"

// Perform pre-flight checks to sufficiently use this module.
func init() {

	var err error

	// zfs check
	// check if the zfs binary exists
	// check if success on `zfs version`
	{
		_, err = os.ReadFile(zfsPath)
		if err != nil {
			log.Printf("%s not found", zfsPath)
			log.Fatal(err)
		}

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
	// check if the zpool binary exists
	// check if success on `zpool version`
	{
		_, err = os.ReadFile(zpoolPath)
		if err != nil {
			log.Printf("%s not found", zpoolPath)
			log.Fatal(err)
		}

		cmd := exec.Command(zpoolPath, "version")
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

// getCommandString returns a string of the command and args of a *exec.Cmd type
func getCommandString(cmd *exec.Cmd) string {
	basename := path.Base(cmd.Path)
	args := strings.Join(cmd.Args[1:], " ")
	return fmt.Sprintf("%s %s", basename, args)
}

// logPipe wraps an io.ReadCloser with a prefixed message and outputs to log.Printf.
// logPipe returns a done channel of type bool to signal when the io.ReadCloser closes.
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
