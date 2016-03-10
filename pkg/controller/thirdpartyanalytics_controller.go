/*
Copyright 2016 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"k8s.io/kubernetes/pkg/api"
	buildapi "github.com/openshift/origin/pkg/build/api"
	deployapi "github.com/openshift/origin/pkg/deploy/api"
	routeapi "github.com/openshift/origin/pkg/route/api"
	templateapi "github.com/openshift/origin/pkg/template/api"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/client/cache"
	osclient "github.com/openshift/origin/pkg/client"
	clientset "k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	"k8s.io/kubernetes/pkg/controller/framework"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/util/workqueue"
	"k8s.io/kubernetes/pkg/watch"

	"github.com/golang/glog"
)

// ThirdPartyAnalyticsController is a controller that synchronizes PersistentVolumeClaims.
type ThirdPartyAnalyticsController struct {
	controllers   map[string]*framework.Controller
	queue         *workqueue.Type
}

// NewThirdPartyAnalyticsController creates a new ThirdPartyAnalyticsController
func NewThirdPartyAnalyticsController(kubeClient clientset.Interface, osClient osclient.Interface) *ThirdPartyAnalyticsController {
	ctrl := &ThirdPartyAnalyticsController{
		controllers: make(map[string]*framework.Controller),
		queue:       workqueue.New(),
	}

	watches := map[string]struct {
		objType   runtime.Object
		listFunc  func(options api.ListOptions) (runtime.Object, error)
		watchFunc func(options api.ListOptions) (watch.Interface, error)
	}{
		// Kubernetes objects
		"pod": {
			objType: &api.Pod{},
			listFunc: func(options api.ListOptions) (runtime.Object, error) {
				return kubeClient.Core().Pods(api.NamespaceAll).List(options)
			},
			watchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return kubeClient.Core().Pods(api.NamespaceAll).Watch(options)
			},
		},
		"replication_controller": {
			objType: &api.ReplicationController{},
			listFunc: func(options api.ListOptions) (runtime.Object, error) {
				return kubeClient.Core().ReplicationControllers(api.NamespaceAll).List(options)
			},
			watchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return kubeClient.Core().ReplicationControllers(api.NamespaceAll).Watch(options)
			},
		},
		"pvclaim": {
			objType: &api.PersistentVolumeClaim{},
			listFunc: func(options api.ListOptions) (runtime.Object, error) {
				return kubeClient.Core().PersistentVolumeClaims(api.NamespaceAll).List(options)
			},
			watchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return kubeClient.Core().PersistentVolumeClaims(api.NamespaceAll).Watch(options)
			},
		},
		"secret": {
			objType: &api.Secret{},
			listFunc: func(options api.ListOptions) (runtime.Object, error) {
				return kubeClient.Core().Secrets(api.NamespaceAll).List(options)
			},
			watchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return kubeClient.Core().Secrets(api.NamespaceAll).Watch(options)
			},
		},
		"service": {
			objType: &api.Service{},
			listFunc: func(options api.ListOptions) (runtime.Object, error) {
				return kubeClient.Core().Services(api.NamespaceAll).List(options)
			},
			watchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return kubeClient.Core().Services(api.NamespaceAll).Watch(options)
			},
		},
		"namespace": {
			objType: &api.Service{},
			listFunc: func(options api.ListOptions) (runtime.Object, error) {
				return kubeClient.Core().Namespaces().List(options)
			},
			watchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return kubeClient.Core().Namespaces().Watch(options)
			},
		},

		// Openshift objects
		"deployment": {
			objType: &deployapi.DeploymentConfig{},
			listFunc: func(options api.ListOptions) (runtime.Object, error) {
				return osClient.DeploymentConfigs(api.NamespaceAll).List(options)
			},
			watchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return osClient.DeploymentConfigs(api.NamespaceAll).Watch(options)
			},
		},
		"route": {
			objType: &routeapi.Route{},
			listFunc: func(options api.ListOptions) (runtime.Object, error) {
				return osClient.Routes(api.NamespaceAll).List(options)
			},
			watchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return osClient.Routes(api.NamespaceAll).Watch(options)
			},
		},
		"build": {
			objType: &buildapi.Build{},
			listFunc: func(options api.ListOptions) (runtime.Object, error) {
				return osClient.Builds(api.NamespaceAll).List(options)
			},
			watchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return osClient.Builds(api.NamespaceAll).Watch(options)
			},
		},
		"template": {
			objType: &templateapi.Template{},
			listFunc: func(options api.ListOptions) (runtime.Object, error) {
				return osClient.Templates(api.NamespaceAll).List(options)
			},
			watchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return osClient.Templates(api.NamespaceAll).Watch(options)
			},
		},
	}

	for name, watch := range watches {
		_, c := framework.NewInformer(
			&cache.ListWatch{
				ListFunc:  watch.listFunc,
				WatchFunc: watch.watchFunc,
			},
			watch.objType,
			0, // 0 is no re-sync
			framework.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					meta, err := meta.Accessor(obj)
					if err != nil {
						glog.Errorf("object has no meta: %v", err)
					}
					ctrl.enqueue(name, "add", meta.GetNamespace())
				},
				UpdateFunc: func(oldObj, newObj interface{}) {
					meta, err := meta.Accessor(newObj)
					if err != nil {
						glog.Errorf("object has no meta: %v", err)
					}
					ctrl.enqueue(name, "update", meta.GetNamespace())
				},
				DeleteFunc: func(obj interface{}) {
					unk, ok := obj.(cache.DeletedFinalStateUnknown)
					if ok {
						obj = unk.Obj
					}
					meta, err := meta.Accessor(obj)
					if err != nil {
						glog.Errorf("object has no meta: %v", err)
					}
					ctrl.enqueue(name, "delete", meta.GetNamespace())
				},
			},
		)
		ctrl.controllers[name] = c
	}
	return ctrl
}

func (c *ThirdPartyAnalyticsController) enqueue(objName, action, namespace string) {
	glog.V(3).Infof("Enqueueing for tracking %s %s %s", objName, action, namespace)
	c.queue.Add(newEvent(objName, action, namespace))
}

// Run starts all the watches within this controller and starts workers to process events
func (c *ThirdPartyAnalyticsController) Run(stopCh <-chan struct{}, workers int) {
	glog.V(5).Infof("Starting ThirdPartyAnalyticsController\n")
	for name, c := range c.controllers {
		glog.V(5).Infof("Starting watch for %s", name)
		go c.Run(stopCh)
	}
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}
}

func (c *ThirdPartyAnalyticsController) track(objName, action, namespace string) error {
	// TODO: All of these values/keys need to come from config
	tracker := NewAnalyticsTracker()
	params := map[string]string{
		"host":                 "dev.openshift.redhat.com",
		"event":                fmt.Sprintf("%s_%s", strings.ToLower(objName), strings.ToLower(action)),
		"cv_email":             namespace,
		"cv_project_namespace": namespace,
	}

	if err := tracker.TrackEvent(params, "GET", "http://www.woopra.com/track/ce?%s"); err != nil {
		return fmt.Errorf("Error sending track event: %v", err)
	}
	return nil
}

// worker runs a worker thread that just dequeues items, processes them, and marks them done.
func (c *ThirdPartyAnalyticsController) worker() {
	for {
		func() {
			obj, quit := c.queue.Get()
			if quit {
				return
			}
			defer c.queue.Done(obj)

			if e, ok := obj.(*analyticsEvent); ok {
				err := c.track(e.objectName, e.action, e.namespace)
				if err != nil {
					glog.Errorf("Error tracking event: %s %s %s %v", e.objectName, e.action, e.namespace, err)
				}
			}
		}()
	}
}

type AnalyticsTracker interface {
	SaveEvent(objName, action, namespace string) error
}

func NewAnalyticsTracker() *realAnalyticsTracker {
	return &realAnalyticsTracker{}
}

type realAnalyticsTracker struct {
}

func (c *realAnalyticsTracker) TrackEvent(params map[string]string, method, endpoint string) error {
	urlParams := url.Values{}
	for key, value := range params {
		urlParams.Add(key, value)
	}
	encodedUrl := urlParams.Encode()
	glog.V(3).Infof("Tracking data: %s", encodedUrl)
	if method == "GET" {
		resp, err := http.Get(fmt.Sprintf(endpoint, encodedUrl))
		//	req.SetBasicAuth(AppID, SecretKey)
		if err != nil {
			return err
		}

		_, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error tracking event: %v", err)
		}
	}
	return nil
}

type analyticsEvent struct {
	objectName string
	action     string
	namespace  string
}

func newEvent(objName, action, namespace string) *analyticsEvent {
	return &analyticsEvent{objName, action, namespace}
}
