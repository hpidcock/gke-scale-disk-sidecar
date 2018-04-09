package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	google_oauth "golang.org/x/oauth2/google"
	google_compute "google.golang.org/api/compute/v1"

	core_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	projectID     string
	namespace     string
	containerName string
	podName       string
	volumeString  string
	threshold     int
	expandBy      int
	pollPeriod    time.Duration

	computeService        *google_compute.Service
	diskService           *google_compute.DisksService
	zoneOperationsService *google_compute.ZoneOperationsService
)

type mountedGCEVolume struct {
	Name        string
	MountedPath string
	DevicePath  string
	PDName      string
	GCPRegion   string
	GCPZone     string
}

func main() {
	ctx := context.Background()

	flag.StringVar(&containerName, "container-name", "", "name of this k8s container")
	flag.StringVar(&podName, "pod-name", "", "name of the parent k8s pod")
	flag.StringVar(&namespace, "namespace", "", "name of the parent k8s namespace")
	flag.StringVar(&volumeString, "volumes", "", "comma seperated list of mounted volumes to expand")
	flag.IntVar(&threshold, "threshold", 80, "usage percentage threshold on a volume to trigger expansion")
	flag.IntVar(&expandBy, "expand-by", 20, "percentage of current volume size to add when expansion is triggered")
	flag.DurationVar(&pollPeriod, "poll-period", 60*time.Second, "period between each poll of disk status")

	flag.Parse()
	if flag.Parsed() == false ||
		containerName == "" ||
		podName == "" ||
		namespace == "" ||
		volumeString == "" {
		flag.PrintDefaults()
		return
	}

	volumes := strings.Split(volumeString, ",")

	client, err := google_oauth.DefaultClient(ctx, google_compute.ComputeScope)
	if err != nil {
		log.Fatal(err)
	}
	computeService, err = google_compute.New(client)
	if err != nil {
		log.Fatal(err)
	}
	diskService = google_compute.NewDisksService(computeService)
	zoneOperationsService = google_compute.NewZoneOperationsService(computeService)

	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	pod, err := clientset.Core().Pods(namespace).Get(podName, meta_v1.GetOptions{})
	if err != nil {
		log.Fatal(err)
	}

	if pod == nil {
		log.Fatal("could not find pod")
	}
	log.Printf("found pod %s", pod.Name)

	node, err := clientset.Core().Nodes().Get(pod.Spec.NodeName, meta_v1.GetOptions{})
	if err != nil {
		log.Fatal(err)
	}

	if node == nil {
		log.Fatal("could not find pod's node")
	}
	log.Printf("found node %s", node.Name)

	uri, err := url.Parse(node.Spec.ProviderID)
	if err != nil {
		log.Fatal(err)
	}

	if uri.Scheme != "gce" {
		log.Fatal("pod running on a non-GKE node")
	}

	projectID = uri.Host
	log.Printf("GCP ProjectID is %s", projectID)

	container, err := findContainer(pod, containerName)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("found container %s", container.Name)

	gceVolumes, err := getMountedVolumes(pod, container, volumes, clientset)
	if err != nil {
		log.Fatal(err)
	}

	if len(gceVolumes) != len(volumes) {
		log.Fatal("was not able to map all volumes")
	}

	for _, volume := range gceVolumes {
		log.Printf("volume %s: GCE PD %s attached as %s mounted to %s", volume.Name, volume.PDName, volume.DevicePath, volume.MountedPath)
	}

	log.Print("starting volume monitor loop")
	for {
		for _, volume := range gceVolumes {
			err := checkFilesystemUsage(volume)
			if err != nil {
				log.Printf("volume %s: %v", volume.Name, err) // Non-fatal, try again next loop
			}
		}
		time.Sleep(pollPeriod)
	}
}

func checkFilesystemUsage(volume mountedGCEVolume) error {
	usage, err := getFilesystemUsage(volume)
	if err != nil {
		return err
	}

	if usage < threshold {
		return nil
	}

	log.Printf("volume %s has passed pressure threshold of %d%% usage", volume.Name, threshold)
	log.Printf("volume %s: attempting to resize filesystem to partition size", volume.Name)

	err = resizeFilesystem(volume)
	if err != nil {
		return err
	}

	usage, err = getFilesystemUsage(volume)
	if err != nil {
		return err
	}

	if usage < threshold {
		log.Printf("volume %s: filesystem resized to partition succesfully relieved pressure", volume.Name)
		return nil
	}

	log.Printf("volume %s: attempting to resize persistent disk to %d%%", volume.Name, 100+expandBy)
	err = resizePersistentDisk(volume)
	if err != nil {
		return err
	}

	time.Sleep(10 * time.Second)

	log.Printf("volume %s: attempting to resize filesystem to partition size", volume.Name)
	err = resizeFilesystem(volume)
	if err != nil {
		return err
	}

	usage, err = getFilesystemUsage(volume)
	if err != nil {
		return err
	}

	if usage < threshold {
		log.Printf("volume %s: persistent disk resized, filesystem resized to partition, succesfully relieved pressure", volume.Name)
		return nil
	}

	return fmt.Errorf("failed to relieve pressure on persistent disk")
}

func resizePersistentDisk(volume mountedGCEVolume) error {
	log.Printf("DisksService: attempting to get PD %s in zone %s project %s", volume.PDName, volume.GCPZone, projectID)
	disk, err := diskService.Get(projectID, volume.GCPZone, volume.PDName).Do()
	if err != nil {
		return err
	}

	log.Printf("DisksService: PD %s is %dGb in size", volume.PDName, disk.SizeGb)

	// Grow by at least 1GB
	expand := math.Max(1, float64(disk.SizeGb)*(float64(expandBy)/100.0))
	newSize := disk.SizeGb + int64(math.Ceil(expand))

	log.Printf("DisksService: attempting to resize PD %s from %dGb to %dGb", volume.PDName, disk.SizeGb, newSize)

	resizeReq := google_compute.DisksResizeRequest{
		SizeGb: newSize,
	}
	op, err := diskService.Resize(projectID, volume.GCPZone, volume.PDName, &resizeReq).Do()
	if err != nil {
		return err
	}

	if op == nil {
		return fmt.Errorf("nil operation returned by GCPDisksService")
	}

	for op.Status != "DONE" || op.Error != nil {
		time.Sleep(30 * time.Second)

		op, err = zoneOperationsService.Get(projectID, volume.GCPZone, op.Name).Do()
		if err != nil {
			return err
		}

		if op == nil {
			return fmt.Errorf("nil operation returned by GCPZoneOperationsService")
		}
	}

	if op.Error != nil {
		merr := &multierror.Error{}
		for _, v := range op.Error.Errors {
			if v == nil {
				continue
			}

			merr = multierror.Append(merr, errors.New(v.Message))
		}

		return multierror.Flatten(merr)
	}

	log.Printf("DisksService: PD %s resized", volume.PDName)
	return nil
}

func resizeFilesystem(volume mountedGCEVolume) error {
	cmd := exec.Command("resize2fs", volume.DevicePath)
	if cmd == nil {
		return fmt.Errorf("could not start resize2fs")
	}

	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

func getFilesystemUsage(volume mountedGCEVolume) (int, error) {
	stat := syscall.Statfs_t{}
	err := syscall.Statfs(volume.MountedPath, &stat)
	if err != nil {
		return 0, err
	}

	usage := int((1.0 - (float64(stat.Bavail) / float64(stat.Blocks))) * 100.0)
	return usage, nil
}

func getMountedVolumes(pod *core_v1.Pod, container *core_v1.Container, volumes []string, clientset *kubernetes.Clientset) ([]mountedGCEVolume, error) {
	gceVolumes := make([]mountedGCEVolume, len(volumes))

	mappedVolumeMounts := mapVolumeMounts(container.VolumeMounts)
	mappedVolumes := mapVolumes(pod.Spec.Volumes)
	for _, volumeName := range volumes {
		volume, ok := mappedVolumes[volumeName]
		if ok == false {
			return nil, fmt.Errorf("volume %s does not exist in pod %s", volumeName, podName)
		}
		volumeMount, ok := mappedVolumeMounts[volumeName]
		if ok == false {
			return nil, fmt.Errorf("volume %s is not mounted to container %s", volumeName, containerName)
		}

		if volume.GCEPersistentDisk != nil {
			return nil, fmt.Errorf("volume %s cannot be a short-hand bound persistent volume, must use PersistentVolumeClaim", volumeName)
		}

		if volume.PersistentVolumeClaim == nil {
			return nil, fmt.Errorf("volume %s is not a GCEPersistentDisk", volumeName)
		}

		pvcName := volume.PersistentVolumeClaim.ClaimName
		log.Printf("attempting to get PersistentVolumeClaim %s", pvcName)
		pvc, err := clientset.Core().
			PersistentVolumeClaims(namespace).
			Get(pvcName, meta_v1.GetOptions{})
		if err != nil {
			return nil, err
		}

		if pvc == nil {
			return nil, fmt.Errorf("could not find PersistentVolumeClaim %s in namespace %s", pvcName, namespace)
		}

		if pvc.Status.Phase != core_v1.ClaimBound {
			return nil, fmt.Errorf("PersistentVolumeClaim %s phase is not Bound, instead %s", pvcName, pvc.Status.Phase)
		}

		pvName := pvc.Spec.VolumeName
		log.Printf("attempting to get PersistentVolume %s", pvName)
		pv, err := clientset.Core().PersistentVolumes().Get(pvName, meta_v1.GetOptions{})
		if err != nil {
			return nil, err
		}

		if pv == nil {
			return nil, fmt.Errorf("could not find PersistentVolume %s which is meant to be bound to PersistentVolumeClaim %s", pvName, pvcName)
		}

		if pv.Status.Phase != core_v1.VolumeBound {
			return nil, fmt.Errorf("volume %s: PV %s phase is not Bound, instead %s", volumeName, pvName, pv.Status.Phase)
		}

		pd := pv.Spec.GCEPersistentDisk
		if pd == nil {
			return nil, fmt.Errorf("volume %s: PV %s is not a GCEPersistentDisk", volumeName, pvName)
		}

		if pv.Labels == nil {
			return nil, fmt.Errorf("volume %s: PV %s is missing labels", volumeName, pvName)
		}

		regionLabel, ok := pv.Labels["failure-domain.beta.kubernetes.io/region"]
		if ok == false {
			return nil, fmt.Errorf("volume %s: PV %s missing failure-domain.beta.kubernetes.io/region label", volumeName, pvName)
		}

		zoneLabel, ok := pv.Labels["failure-domain.beta.kubernetes.io/zone"]
		if ok == false {
			return nil, fmt.Errorf("volume %s: PV %s missing failure-domain.beta.kubernetes.io/zone label", volumeName, pvName)
		}

		if pd.Partition != 0 {
			return nil, fmt.Errorf("volume %s: PD %s has more than one parition", volumeName, pd.PDName)
		}

		if pd.ReadOnly == true {
			return nil, fmt.Errorf("volume %s: PD %s is read only", volumeName, pd.PDName)
		}

		if pd.FSType != "" && pd.FSType != "ext4" {
			return nil, fmt.Errorf("volume %s: PD %s is not a ext4 volume", volumeName, pd.PDName)
		}

		log.Printf("attempting to resolve device path for %s", volumeMount.MountPath)
		devicePath, err := resolveDevicePath(volumeMount.MountPath)
		if err != nil {
			return nil, nil
		}

		if devicePath == "" {
			return nil, fmt.Errorf("could not resolve device path for volume %s", volumeName)
		}

		gceVolumes = append(gceVolumes, mountedGCEVolume{
			Name:        volumeName,
			MountedPath: volumeMount.MountPath,
			DevicePath:  devicePath,
			PDName:      pd.PDName,
			GCPRegion:   regionLabel,
			GCPZone:     zoneLabel,
		})
	}

	return gceVolumes, nil
}

func resolveDevicePath(mountPath string) (string, error) {
	cmd := exec.Command("findmnt", "-o", "source", "--noheadings", mountPath)
	if cmd == nil {
		return "", fmt.Errorf("could not start findmnt")
	}

	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()
	if err != nil {
		return "", err
	}

	path := filepath.Clean(out.String())
	if _, err = os.Stat(path); os.IsNotExist(err) {
		return "", fmt.Errorf("mount target %s yielded no existent device %s", mountPath, path)
	}

	return path, nil
}

func mapVolumeMounts(volumeMounts []core_v1.VolumeMount) map[string]core_v1.VolumeMount {
	vm := make(map[string]core_v1.VolumeMount)
	for _, v := range volumeMounts {
		vm[v.Name] = v
	}
	return vm
}

func mapVolumes(volumes []core_v1.Volume) map[string]core_v1.Volume {
	vm := make(map[string]core_v1.Volume)
	for _, v := range volumes {
		vm[v.Name] = v
	}
	return vm
}

func findContainer(pod *core_v1.Pod, name string) (*core_v1.Container, error) {
	if pod == nil {
		return nil, errors.New("pod is nil")
	}

	for _, container := range pod.Spec.Containers {
		if container.Name == name {
			return &container, nil
		}
	}

	return nil, errors.New("container not found")
}
