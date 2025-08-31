// hd-idle - spin down idle hard disks
// Copyright (C) 2018  Andoni del Olmo
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"fmt"
	"github.com/adelolmo/hd-idle/io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	defaultIdleTime     = 600 * time.Second
	symlinkResolveOnce  = 0
	symlinkResolveRetry = 1
)

func main() {

	if os.Getenv("START_HD_IDLE") == "false" {
		fmt.Println("START_HD_IDLE=false exiting now.")
		os.Exit(0)
	}

	singleDiskMode := false
	testMode := false
	var disk string
	defaultConf := DefaultConf{
		Idle:           defaultIdleTime,
		CommandType:    SCSI,
		PowerCondition: 0,
		Debug:          false,
		SymlinkPolicy:  0,
		SkipIfMounted:  false,
	}
	var config = &Config{
		Devices:  []DeviceConf{},
		Defaults: defaultConf,
		NameMap:  map[string]string{},
	}
	var deviceConf *DeviceConf

	if len(os.Args) == 0 {
		usage()
		os.Exit(1)
	}

	for index, arg := range os.Args[1:] {
		switch arg {
		case "-t":
			var err error
			disk, err = argument(index)
			if err != nil {
				fmt.Println("Missing disk argument after -t. Must be a device (e.g. -t sda).")
				os.Exit(1)
			}
			singleDiskMode = true

		case "-T":
			var err error
			disk, err = argument(index)
			if err != nil {
				fmt.Println("Missing disk argument after -T. Must be a device (e.g. -T sda).")
				os.Exit(1)
			}
			testMode = true

		case "-s":
			s, err := argument(index)
			if err != nil {
				fmt.Println("Missing symlink_policy. Must be 0 or 1.")
				os.Exit(1)
			}
			switch s {
			case "0":
				config.Defaults.SymlinkPolicy = symlinkResolveOnce
			case "1":
				config.Defaults.SymlinkPolicy = symlinkResolveRetry
			default:
				fmt.Printf("Wrong symlink_policy -s %s. Must be 0 or 1.\n", s)
				os.Exit(1)
			}

		case "-a":
			if deviceConf != nil {
				config.Devices = append(config.Devices, *deviceConf)
			}

			name, err := argument(index)
			if err != nil {
				fmt.Println("Missing disk argument after -a. Must be a device (e.g. -a sda).")
				os.Exit(1)
			}

			deviceRealPath, err := io.RealPath(name)
			if err != nil {
				deviceRealPath = ""
				fmt.Printf("Unable to resolve symlink: %s\n", name)
			}
			deviceConf = &DeviceConf{
				Name:           deviceRealPath,
				GivenName:      name,
				Idle:           config.Defaults.Idle,
				CommandType:    config.Defaults.CommandType,
				PowerCondition: config.Defaults.PowerCondition,
			}
			config.NameMap[deviceRealPath] = name
			deviceConf.SkipIfMounted = config.Defaults.SkipIfMounted

		case "-i":
			s, err := argument(index)
			if err != nil {
				fmt.Println("Missing idle_time after -i. Must be a number.")
				os.Exit(1)
			}
			idle, err := strconv.Atoi(s)
			if err != nil {
				fmt.Printf("Wrong idle_time -i %d. Must be a number.", idle)
				os.Exit(1)
			}
			if deviceConf == nil {
				config.Defaults.Idle = time.Duration(idle) * time.Second
				break
			}
			deviceConf.Idle = time.Duration(idle) * time.Second

		case "-I":
			config.Defaults.IgnoreSpinDownDetection = true

		case "-c":
			command, err := argument(index)
			if err != nil {
				fmt.Println("Missing command_type after -c. Must be one of: scsi, ata.")
				os.Exit(1)
			}
			switch command {
			case SCSI, ATA:
				if deviceConf == nil {
					config.Defaults.CommandType = command
					break
				}
				deviceConf.CommandType = command
			default:
				fmt.Printf("Wrong command_type -c %s. Must be one of: scsi, ata.", command)
				os.Exit(1)
			}

		case "-p":
			s, err := argument(index)
			if err != nil {
				fmt.Println("Missing power condition after -p. Must be a number from 0-15.")
				os.Exit(1)
			}
			powerCondition, err := strconv.ParseUint(s, 0, 4)
			if err != nil {
				fmt.Printf("Invalid power condition %s: %s", s, err.Error())
				os.Exit(1)
			}
			if deviceConf == nil {
				config.Defaults.PowerCondition = uint8(powerCondition)
				break
			}
			deviceConf.PowerCondition = uint8(powerCondition)

		case "-l":
			logfile, err := argument(index)
			if err != nil {
				fmt.Println("Missing logfile after -l.")
				os.Exit(1)
			}
			config.Defaults.LogFile = logfile

		case "-d":
			config.Defaults.Debug = true

		case "-M":
			config.Defaults.SkipIfMounted = true
			if deviceConf != nil {
				deviceConf.SkipIfMounted = true
			}

		case "-h":
			usage()
			os.Exit(0)
		}
	}

	if singleDiskMode {
		if err := spindownDisk(
			disk,
			config.Defaults.CommandType,
			config.Defaults.PowerCondition,
			config.Defaults.Debug,
		); err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}

	if testMode {
		// Check if device is part of a mounted btrfs or zfs filesystem
		if isBtrfs, btrfsInfo := checkBtrfsDevice(disk); isBtrfs {
			fmt.Println(btrfsInfo)
			os.Exit(0)
		}
		if isZfs, zfsInfo := checkZfsDevice(disk); isZfs {
			fmt.Println(zfsInfo)
			os.Exit(0)
		}
		fmt.Printf("%s is not part of any mounted Btrfs or ZFS filesystem.\n", disk)
		os.Exit(0)
	}

	if deviceConf != nil {
		config.Devices = append(config.Devices, *deviceConf)
	}
	fmt.Println(config.String())

	interval := poolInterval(config.Devices)
	config.SkewTime = interval * 3
	for {
		ObserveDiskActivity(config)
		time.Sleep(interval)
	}
}

func argument(index int) (string, error) {
	argIndex := index + 2
	if argIndex >= len(os.Args) {
		return "", fmt.Errorf("option requires argument")
	}
	arg := os.Args[argIndex]
	if arg[:1] == "-" {
		return "", fmt.Errorf("option requires argument")
	}
	return arg, nil
}

func usage() {
	fmt.Println(`usage: hd-idle [options]

Options:
  -t <disk>             Test mode: spindown the specified disk immediately.
  -T <disk>             Test mode: check if the specified disk is part of a mounted Btrfs or ZFS filesystem and exit.
  -s <symlink_policy>   Symlink policy: 0 (resolve once), 1 (resolve on retry). Default is 0.
  -a <name>             Add a device to monitor. Can be a device name (e.g., sda) or a symlink (e.g., /dev/disk/by-id/...).
                        This option can be used multiple times for multiple devices.
  -i <idle_time>        Idle time in seconds before spindown. Applies to the last specified device (-a) or as a default.
  -c <command_type>     Command type for spindown: 'scsi' or 'ata'. Applies to the last specified device (-a) or as a default.
  -p <power_condition>  Power condition for SCSI devices (0-15). Applies to the last specified device (-a) or as a default.
  -l <logfile>          Path to a log file for recording spinup events.
  -d                    Enable debug output.
  -I                    Ignore prior spindown state (force spindown even if already spun down).
  -M                    Skip spindown if the device is currently mounted. Applies to the last specified device (-a) or as a default.
  -h                    Display this help message.`)
}

func poolInterval(deviceConfs []DeviceConf) time.Duration {
	if len(deviceConfs) == 0 {
		return defaultIdleTime / 10
	}

	interval := defaultIdleTime
	for _, dev := range deviceConfs {
		if dev.Idle == 0 {
			continue
		}
		if dev.Idle < interval {
			interval = dev.Idle
		}
	}

	sleepTime := interval / 10
	if sleepTime == 0 {
		return time.Second
	}
	return sleepTime
}

// checkBtrfsDevice checks if a device is part of a mounted Btrfs filesystem
func checkBtrfsDevice(device string) (bool, string) {
	// Check if device is valid
	if !fileExists(device) {
		return false, fmt.Sprintf("Error: %s is not a valid block device.", device)
	}

	// Get mounted btrfs filesystems
	mounts, err := exec.Command("mount", "-t", "btrfs").Output()
	if err != nil {
		return false, fmt.Sprintf("%s is not part of any mounted Btrfs filesystem.", device)
	}

	// Parse mount points
	mountPoints := parseMountPoints(string(mounts))
	if len(mountPoints) == 0 {
		return false, fmt.Sprintf("%s is not part of any mounted Btrfs filesystem.", device)
	}

	// Check each mount point
	for _, mountPoint := range mountPoints {
		// Run btrfs filesystem show for this mount point
		cmd := exec.Command("btrfs", "filesystem", "show", mountPoint)
		output, err := cmd.CombinedOutput()
		if err != nil {
			continue
		}

		// Check if device is in the output
		if strings.Contains(string(output), device) {
			// Check if device is marked as missing
			if strings.Contains(string(output), device+" missing") {
				return true, fmt.Sprintf("%s is part of a Btrfs filesystem at %s but is marked as MISSING.", device, mountPoint)
			}
			return true, fmt.Sprintf("%s is part of a mounted Btrfs filesystem at %s.\nStatus: %s is active in the Btrfs filesystem.", device, mountPoint, device)
		}
	}

	return false, fmt.Sprintf("%s is not part of any mounted Btrfs filesystem.", device)
}

// checkZfsDevice checks if a device is part of a ZFS pool
func checkZfsDevice(device string) (bool, string) {
	// Check if device is valid
	if !fileExists(device) {
		return false, fmt.Sprintf("Error: %s is not a valid block device.", device)
	}

	// Get device serial
	devID := getDeviceSerial(device)

	// Get partitions
	partitions := getDevicePartitions(device)

	// Get ZFS pools
	pools, err := exec.Command("zpool", "list", "-H", "-o", "name").Output()
	if err != nil {
		return false, fmt.Sprintf("%s is not part of any ZFS pool.", device)
	}

	// Parse pool names
	poolNames := strings.Fields(string(pools))
	if len(poolNames) == 0 {
		return false, fmt.Sprintf("%s is not part of any ZFS pool.", device)
	}

	// Check each pool
	for _, pool := range poolNames {
		// Build pattern for grep
		pattern := device
		if devID != "" {
			pattern += "|" + devID
		}
		for _, part := range partitions {
			pattern += "|" + part
		}

		// Check if device is in zpool status
		cmd := exec.Command("zpool", "status", pool)
		output, err := cmd.CombinedOutput()
		if err != nil {
			continue
		}

		// Check if device matches pattern
		matched, _ := regexp.MatchString(pattern, string(output))
		if matched {
			// Check if pool is mounted
			mountOutput, _ := exec.Command("mount").Output()
			if strings.Contains(string(mountOutput), "zfs") && strings.Contains(string(mountOutput), pool) {
				// Get mount point
				lines := strings.Split(string(mountOutput), "\n")
				var mpFound string
				for _, line := range lines {
					if strings.Contains(line, "zfs") && strings.Contains(line, pool) {
						fields := strings.Fields(line)
						if len(fields) > 2 {
							mpFound = fields[2]
							break
						}
					}
				}

				// Get device state
				lines = strings.Split(string(output), "\n")
				var state string
				for _, line := range lines {
					if regexp.MustCompile(pattern).MatchString(line) {
						fields := strings.Fields(line)
						if len(fields) > 1 {
							state = fields[1]
							break
						}
					}
				}

				if state == "ONLINE" {
					return true, fmt.Sprintf("%s is part of a mounted ZFS filesystem in pool %s at %s.\nStatus: %s is active in the ZFS pool.", device, pool, mpFound, device)
				}
				return true, fmt.Sprintf("%s is part of a ZFS pool %s but is in state %s.", device, pool, state)
			}
			return true, fmt.Sprintf("%s is part of ZFS pool %s but the pool is not mounted.", device, pool)
		}
	}

	return false, fmt.Sprintf("%s is not part of any ZFS pool.", device)
}

// Helper functions
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func parseMountPoints(mounts string) []string {
	var mountPoints []string
	lines := strings.Split(mounts, "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 3 {
			mountPoints = append(mountPoints, fields[2])
		}
	}
	return mountPoints
}

func getDeviceSerial(device string) string {
	cmd := exec.Command("lsblk", "-no", "SERIAL", device)
	output, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func getDevicePartitions(device string) []string {
	var partitions []string
	cmd := exec.Command("lsblk", "-ln", "-o", "NAME", device)
	output, err := cmd.Output()
	if err != nil {
		return partitions
	}

	deviceName := filepath.Base(device)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, deviceName) && len(line) > len(deviceName) {
			partitions = append(partitions, "/dev/"+line)
		}
	}
	return partitions
}
