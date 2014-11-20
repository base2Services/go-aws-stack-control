package awsstackcontrol

import (
	"time"
	"errors"
	"strconv"
	"net/http"
	"log"
	"encoding/json"
	aws "github.com/base2Services/go-b2aws"
)

type ActionCallBacks interface {
	NoSuchEnvironment()
	MisingOrderTags()
	TierShutdown()
	StackShutdown()
	TierStartedup()
	StackStartedup()
	TierTakingTooLong()
}

func ExtractTags(instance aws.Instance) (environment, stack, name, startOrder, stopOrder string) {
	for _, tag := range instance.Tags {
		if tag.Key == "Name" {
			name = tag.Value
		} else if tag.Key == "Stack" {
			stack = tag.Value
		} else if tag.Key == "Environment" {
			environment = tag.Value
		} else if tag.Key == "StartOrder" {
			startOrder = tag.Value
		} else if tag.Key == "StopOrder" {
			stopOrder = tag.Value
		}
	}
	return
}

func GetInstanceGroupTeirMap(l log.Logger, regionMap map[string]string, regionMap map[string]string, stack string, environment string, profileName string) (tiered_instances map[string][]aws.Instance, max_order_pos int) {
	var w interface{
		Header() http.Header
		Write(_ []byte) (int, error)
		WriteHeader(_ int)
	}

	all_instances := aws.GetAllInstancesFutures(regionMap, regionMap, client, w, c)
	tiered_instances = make(map[string][]aws.Instance)

	max_order_pos = 0

	for _, instance := range all_instances {
		instance_environment, instance_stack, _, _, stopOrder := ExtractTags(instance)

		c.Infof("For instance %s, found %s, %s, %s", instance.InstanceId, instance_environment, instance_stack, stopOrder)
		if instance.ProfileName == profileName && environment == instance_environment && stack == instance_stack {
			stopOrderInt, err := strconv.Atoi(stopOrder)
			if err == nil {
				if stopOrderInt > max_order_pos {
					max_order_pos = stopOrderInt
				}
			}
			instance_list, ok := tiered_instances[stopOrder]
			if !ok {
				instance_list = make([]aws.Instance,0)
			}
			instance_list = append(instance_list, instance)
			tiered_instances[stopOrder] = instance_list
		}
	}
	return
}

func TeiredInstanceExecute(l log.Logger, regionMap map[string]string, tiered_instances map[string][]aws.Instance, max_order_pos int, lambda func (ids []string, regionUrl string, successChannel chan aws.StartInstance)) {
	for i := 1; i <= max_order_pos; i++ {
		c.Infof("Invoking tier: %d", i)
		successChannel := make (chan aws.StartInstance)
		if instances, ok := tiered_instances[strconv.Itoa(i)]; ok {
			c.Infof("Number of instances: %d", len(instances))
			regionIds := make(map[string][]string)
			regions := []string{}
			for _, instance := range instances {
				c.Infof("Invoking instance: %s", instance.InstanceId)

				regionUrl, _ := regionMap[instance.Region]

				instance_list, ok := regionIds[regionUrl]
				if !ok {
					instance_list = []string{}
					regions = append(regions, regionUrl)
				}
				instance_list = append(instance_list, instance.InstanceId)
				regionIds[regionUrl] = instance_list
			}
			for _, regionUrl := range regions {
				go lambda(regionIds[regionUrl], regionUrl, successChannel)
			}
			for i, _ := range instances {
				c.Infof("Got success %d", i)
				result := <- successChannel
				c.Infof("Got success %s", result)
			}
		}
		close(successChannel)
	}
}

func ShutdownEnvironment(l log.Logger, client *http.Client, regionMap map[string]string, stack string, environment string, profileName string, publicKey string, secretKey string, callback ActionCallBacks) {
	// Get instances and filter for the environments
	tiered_instances, max_order_pos := GetInstanceGroupTeirMap(l, client, regionMap, stack, environment, profileName)

	tmp,_ := json.Marshal(tiered_instances)
	c.Infof("%s", tmp)
	c.Infof("Max int: %d", max_order_pos)

	// validate we can shut them down
	if instance_list, ok := tiered_instances[""]; ok && len(instance_list) > 0 {
		callback.NoSuchEnvironment()
		return
	}
	if max_order_pos < 1 {
		// Nothing to do, no shutdown order
		callback.MisingOrderTags()
		return
	}

	// Loop through each milestone shutting down instances in parallel
	TeiredInstanceExecute(l, regionMap, tiered_instances, max_order_pos, func (ids []string, regionUrl string, successChannel chan aws.StartInstance) {
			instances, _, _ := aws.StopInstances(publicKey, secretKey, regionUrl, client, nil, ids...)
			err := WaitUntilInstanceStatusIs(l, publicKey, secretKey, regionUrl, client, nil, "stopped", ids...)
			if err == nil {
				callback.TierShutdown()
			} else {
				callback.TierTakingTooLong()
			}
			successChannel <- instances
		})

	callback.StackShutdown()
}

func StartupEnvironment(l log.Logger, client *http.Client, regionMap map[string]string, stack string, environment string, profileName string, publicKey string, secretKey string, callback ActionCallBacks) {
	// Get instances and filter for the environments

	tiered_instances, max_order_pos := GetInstanceGroupTeirMap(l, client, regionMap, stack, environment, profileName)

	tmp,_ := json.Marshal(tiered_instances)
	c.Infof("%s", tmp)
	c.Infof("Max int: %d", max_order_pos)

	// validate we can shut them down
	if instance_list, ok := tiered_instances[""]; ok && len(instance_list) > 0 {
		callback.NoSuchEnvironment()
		return
	}
	if max_order_pos < 1 {
		// Nothing to do, no shutdown order
		callback.MisingOrderTags()
		return
	}

	// Loop through each milestone shutting down instances in parallel
	TeiredInstanceExecute(l, regionMap, tiered_instances, max_order_pos, func (ids []string, regionUrl string, successChannel chan aws.StartInstance) {
			instances, _, _ := aws.StartInstances(publicKey, secretKey, regionUrl, client, nil, ids...)
			err := WaitUntilInstanceStatusIs(l, client, publicKey, secretKey, regionUrl, client, nil, "running", ids...)
			if err == nil {
				callback.TierStartedup()
			} else {
				callback.TierTakingTooLong()
			}
			successChannel <- instances
		})

	callback.StackStartedup()
}

func WaitUntilInstanceStatusIs(l log.Logger, client *http.Client, accessKey string, secretKey string, regionEndpoint string, client *http.Client, w http.ResponseWriter, status string, instanceIds ...string) (err error) {
	for tries := 0;;tries++ {
		time.Sleep(time.Second * 30)
		instantStatuses, err := aws.GetInstancesStatus(accessKey, secretKey, regionEndpoint, client, nil, true, instanceIds...)
		if err != nil {
			c.Infof("Lookup error: %s\n", err)
			continue;
		}
		i := 0
		for _, instance := range instantStatuses.Instances {
			c.Infof("%s does %s == %s", instance.InstanceId, status, instance.InstanceState)
			if instance.InstanceState != status {
				i++;
			}
		}
		if i == 0 {
			break;
		}
		if tries > 1000 {
			err = errors.New("Wait timeout exceeded stack action")
			return err
		}
	}
	return
}
