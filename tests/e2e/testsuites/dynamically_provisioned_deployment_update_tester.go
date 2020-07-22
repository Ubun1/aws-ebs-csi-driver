package testsuites

import (
	"github.com/c2devel/aws-ebs-csi-driver/tests/e2e/driver"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type DynamicallyProvisionedDeploymentUpdateTest struct {
	CSIDriver driver.DynamicPVTestDriver
	Pod       PodDetails
}

func (t *DynamicallyProvisionedDeploymentUpdateTest) Run(client clientset.Interface, namespace *v1.Namespace) {
	tdeployment, cleanup := t.Pod.SetupUpdatableDeployment(client, namespace, t.csiDriver)
	By("creating the deployment")
	tdeployment.Create()
	//saveVolumeName
	oldVolumeName := tdeployment.deployment.Spec.Template.Spec.Volumes[0].Name

	for i := range cleanup {
		defer cleanup[i]()
	}

	By("update deployment")
	tdeployment.Update()
	tdeployment.WaitForPodReady()

	newVolumeName := tdeployment.deployment.Spec.Template.Spec.Volumes[0].Name
	By("check disks status")
	Expect(oldVolumeName).To(ContainSubstring(newVolumeName))
}
