# Testing

Типы тестов, которые есть в этой репе:
- unit
- e2e

## unit

Запускаются через ```make test```. Используют встроенную в го поддержку тестирования [вот](https://golang.org/doc/code.html#Testing).

## e2e

В апстриме используетс сложный агрегат под названием [aws-k8s-tester](https://github.com/aws/aws-k8s-tester). Этот инструкмент создает кластер, деплоит в него драйвер и запускает тесты. Инструкции по запуску тестов находятся в tester/*.yaml в секции test. Например в single-az-conifg.yaml инструкции следующие:

```
go get -u github.com/onsi/ginkgo/ginkgo
export KUBECONFIG=$HOME/.kube/config
export AWS_AVAILABILITY_ZONES=us-west-2a
$(go env GOBIN)/ginkgo -p -nodes=32 -v --focus="\[ebs-csi-e2e\] \[single-az\]" tests/e2e -- -report-dir=$ARTIFACTS
```
Как видно, это башовые командочки которые можно выполнить и без старшной штуки aws-k8s-tester.

Структура e2e тестов:
```
|-- e2e
|   |-- driver
|   |   |-- driver.go
|   |   `-- ebs_csi_driver.go
|   |-- dynamic_provisioning.go
|   |-- pre_provsioning.go
|   |-- README.md
|   |-- reports
|   |   `-- junit_01.xml
|   |-- suite_test.go
|   `-- testsuites
|       |-- dynamically_provisioned_cmd_volume_tester.go
|       |-- dynamically_provisioned_collocated_pod_tester.go
|       |-- dynamically_provisioned_delete_pod_tester.go
|       |-- dynamically_provisioned_read_only_volume_tester.go
|       |-- dynamically_provisioned_reclaim_policy_tester.go
|       |-- dynamically_provisioned_topology_aware_volume_tester.go
|       |-- dynamically_provisioned_volume_snapshot_tester.go
|       |-- pre_provisioned_read_only_volume_tester.go
|       |-- pre_provisioned_reclaim_policy_tester.go
|       |-- pre_provisioned_volume_tester.go
|       |-- specs.go
|       `-- testsuites.go
|-- e2e-migration
|   |-- e2e_test.go
|   |-- go.mod
|   |-- go.sum
|   `-- README.md
`-- integration
    |-- integration_test.go
    |-- README.md
    `-- setup_test.go
```	

В директории tests есть e2e/e2e-migration/intergration субдиректории. Основная масса тестов находится в e2e. Тесты в е2е функциональные, в основном работют в api k8s и облака (лучшего облака - ц2 облака).

Как запустить тесты
Для запуска тестов нам понадобится.
- [создать](https://docs.cloud.croc.ru/ru/services/kubernetes.html#creating) бубернетес кластер в ц2 кдауде
- попасть по ссш на мастер ноду и выполнить ```sudo -i```
- проверить что в руте настроен kubectl - выполнить: ```kubectl get nodes```
- установить голанг:
- - cd /tmp && curl -O https://dl.google.com/go/go1.13.3.linux-amd64.tar.gz
- - tar -xzf go1.13.3.linux-amd64.tar.gz
- - mv go /usr/local
- - export GOROOT=/usr/local/go
- - export PATH=$GOROOT/bin:$PATH
- - cd -
- установить gcc (нужно для ginkgo):
- - yum install gcc
- склонить эту репу:
- - git clone https://github.com/c2devel/aws-ebs-csi-driver.git
- задать переменные окружения для подлкючения тестов к облаку:
- - export AWS_EC2_ENDPOINT="https://api.cloud.croc.ru"
- - export AWS_AVAILABILITY_ZONES="ru-msk-comp1p"
- - export AWS_SECRET_ACCESS_KEY="<secret_key>"
- - export AWS_ACCESS_KEY_ID="<access_key>"
- установить ginkgo:
- - go get -u github.com/onsi/ginkgo/ginkgo
- задать переменные окружения для подлючения тестов к k8s:
- - export KUBECONFIG=$HOME/.kube/config 
- запустить юнит тесты (проверить что код собирается)
- - cd <repo_base_dir>
- - make test
- запустить e2e тесты:
- - ~/go/bin/ginkgo -v -progress --focus="\[ebs-csi-e2e\] \[single-az\]" <repo_base_dir>/tests/e2e -- -report-dir=./reports/

Какие тесты есть:

Красные:
- "should create a pod, write and read to it, take a volume snapshot, and create another pod from the snapshot" - снапшоттинг не реализован

Пропущенные:
- все с тегами multi-az

Зеленые:
- "should create a volume on demand with volume type %q and fs type %q", volumeType, fsType
- "should create a volume on demand with volumeType %q and encryption", volumeType
- "should create a volume on demand with provided mountOptions"
- "should create multiple PV objects, bind to PVCs and attach all to a single pod"
- "should create multiple PV objects, bind to PVCs and attach all to different pods"
- "should create a raw block volume on demand"
- "should create a raw block volume and a filesystem volume on demand and bind to the same pod"
- "should create multiple PV objects, bind to PVCs and attach all to different pods on the same node"
- "should create a volume on demand and mount it as readOnly in a pod"
- "should delete PV with reclaimPolicy %q", v1.PersistentVolumeReclaimDelete
- "[env] should retain PV with reclaimPolicy %q", v1.PersistentVolumeReclaimRetain
- "should create a deployment object, write and read to it, delete the pod and write and read to it again"
- "should allow for topology aware volume scheduling"
- "[env] should allow for topology aware volume with specified zone in allowedTopologies"
- "[env] should write and read to a pre-provisioned volume"
- "[env] should use a pre-provisioned volume and mount it as readOnly in a pod"
- "[env] should use a pre-provisioned volume and retain PV with reclaimPolicy %q", v1.PersistentVolumeReclaimRetain
- "[env] should use a pre-provisioned volume and delete PV with reclaimPolicy %q", v1.PersistentVolumeReclaimDelete"
