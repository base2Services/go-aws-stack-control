package awsstackcontrol

import (
	"time"
	"errors"
	"strconv"
	"net/http"
	"appengine"
	"encoding/json"
	"appengine/urlfetch"
	"appengine/channel"
	aws "github.com/base2Services/go-b2aws"
)


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

func GetInstanceGroupTeirMap(c appengine.Context, client *http.Client, session *Session, stack string, environment string, profile Creds) (tiered_instances map[string][]aws.Instance, max_order_pos int) {
	regionMap := session.RegionMap
	var w interface{
		Header() http.Header
		Write(_ []byte) (int, error)
		WriteHeader(_ int)
	}

	all_instances := getAllInstancesFutures(regionMap, session, client, w, c)
	tiered_instances = make(map[string][]aws.Instance)

	max_order_pos = 0

	for _, instance := range all_instances {
		instance_environment, instance_stack, _, _, stopOrder := ExtractTags(instance)

		c.Infof("For instance %s, found %s, %s, %s", instance.InstanceId, instance_environment, instance_stack, stopOrder)
		if instance.ProfileName == profile.Name && environment == instance_environment && stack == instance_stack {
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

func TeiredInstanceExecute(c appengine.Context, session *Session, tiered_instances map[string][]aws.Instance, max_order_pos int, lambda func (ids []string, regionUrl string, successChannel chan aws.StartInstance)) {
	for i := 1; i <= max_order_pos; i++ {
		c.Infof("Invoking tier: %d", i)
		successChannel := make (chan aws.StartInstance)
		if instances, ok := tiered_instances[strconv.Itoa(i)]; ok {
			c.Infof("Number of instances: %d", len(instances))
			regionIds := make(map[string][]string)
			regions := []string{}
			for _, instance := range instances {
				c.Infof("Invoking instance: %s", instance.InstanceId)

				regionUrl, _ := session.RegionMap[instance.Region]

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

func ShutdownEnvironment(c appengine.Context, session *Session, stack string, environment string, profile Creds) {
	// Get instances and filter for the environments
	client := urlfetch.Client(c)

	tiered_instances, max_order_pos := GetInstanceGroupTeirMap(c, client, session, stack, environment, profile)

	tmp,_ := json.Marshal(tiered_instances)
	c.Infof("%s", tmp)
	c.Infof("Max int: %d", max_order_pos)

	// validate we can shut them down
	if instance_list, ok := tiered_instances[""]; ok && len(instance_list) > 0 {
		channel.SendJSON(c, session.Token, map[string]string {
			"Error": "Can not startup or shutdown environment.\nEnsure all tags are in use.",
		})
		return
	}
	if max_order_pos < 1 {
		// Nothing to do, no shutdown order
		channel.SendJSON(c, session.Token, map[string]string {
			"Error": "No startup or shutdown tags filled, or all are below 1.",
		})
		return
	}

	// Loop through each milestone shutting down instances in parallel
	TeiredInstanceExecute(c, session, tiered_instances, max_order_pos, func (ids []string, regionUrl string, successChannel chan aws.StartInstance) {
			instances, _, _ := aws.StopInstances(profile.Pkey, profile.Skey, regionUrl, client, nil, ids...)
			err := WaitUntilInstanceStatusIs(c, profile.Pkey, profile.Skey, regionUrl, client, nil, "stopped", ids...)
			if err == nil {
				channel.SendJSON(c, session.Token, map[string]string {
					"Action": "Refresh",
					"Message": "Teir Shutdown",
				})
			} else {
				channel.SendJSON(c, session.Token, map[string]string {
					"Error": "Teir taking to long skipping to next",
				})
			}
			successChannel <- instances
		})

	channel.SendJSON(c, session.Token, map[string]string {
		"Action": "Refresh",
		"Message": "Stack Shutdown",
	})
}

func StartupEnvironment(c appengine.Context, session *Session, stack string, environment string, profile Creds) {
	// Get instances and filter for the environments
	client := urlfetch.Client(c)

	tiered_instances, max_order_pos := GetInstanceGroupTeirMap(c, client, session, stack, environment, profile)

	tmp,_ := json.Marshal(tiered_instances)
	c.Infof("%s", tmp)
	c.Infof("Max int: %d", max_order_pos)

	// validate we can shut them down
	if instance_list, ok := tiered_instances[""]; ok && len(instance_list) > 0 {
		channel.SendJSON(c, session.Token, map[string]string {
			"Error": "Can not startup or shutdown environment.\nEnsure all tags are in use.",
		})
		return
	}
	if max_order_pos < 1 {
		// Nothing to do, no shutdown order
		channel.SendJSON(c, session.Token, map[string]string {
			"Error": "No startup or shutdown tags filled, or all are below 1.",
		})
		return
	}

	// Loop through each milestone shutting down instances in parallel
	TeiredInstanceExecute(c, session, tiered_instances, max_order_pos, func (ids []string, regionUrl string, successChannel chan aws.StartInstance) {
			instances, _, _ := aws.StartInstances(profile.Pkey, profile.Skey, regionUrl, client, nil, ids...)
			err := WaitUntilInstanceStatusIs(c, profile.Pkey, profile.Skey, regionUrl, client, nil, "running", ids...)
			if err == nil {
				channel.SendJSON(c, session.Token, map[string]string {
					"Action": "Refresh",
					"Message": "Teir startup",
				})
			} else {
				channel.SendJSON(c, session.Token, map[string]string {
					"Error": "Teir taking to long skipping to next",
				})
			}
			successChannel <- instances
		})

	channel.SendJSON(c, session.Token, map[string]string {
		"Action": "Refresh",
		"Message": "Stack startup",
	})
}

func WaitUntilInstanceStatusIs(c appengine.Context, accessKey string, secretKey string, regionEndpoint string, client *http.Client, w http.ResponseWriter, status string, instanceIds ...string) (err error) {
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
