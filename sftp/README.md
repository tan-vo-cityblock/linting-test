# SFTP Drop Service

The SFTP drop service runs an SFTP server container and a gsutil
synchronization container inside of a Kubernetes
[Pod](https://kubernetes.io/docs/concepts/workloads/pods/pod/). Every
300 seconds, the gsutil rsync command runs, utilizing the service
account that lives in the `sftp-gcs-sync-svc-account` Kubernetes
[Secret](https://kubernetes.io/docs/concepts/configuration/secret/).

## Connection Details for Partners

Host - sftp.internal.cityblock.engineering
Port - 22
User ID - <insert correct username>
Folder - /drop

## Installation

To install this into a Kubernetes cluster, simply run `kubectl apply  -f all.yml`. 
This will create the deployment, which will create the
pods and keep them running. It also creates a Load Balancer which
handles routing of SSH packets to the Pod, along with static IP
assignment and IP filtering.

You'll also need to create an `sftp-users` secret.

```
kubectl create secret generic --from-literal=SFTP_USERS=$SFTP_USERS
```

where `$SFTP_USERS` is a space-separated string of user configs, which
have the format

```
$username::::$drop
```

for ssh/rsa users and

```
$username:$password:::$drop
```

for username/password users.

### Upgrading

If you make changes to `all.yml`, you can re-apply the file to update
the cluster with `kubectl apply -f all.yml`, which will reconfigure
the cluster and its GCP dependencies.

Run `kubectl rollout status deployment/sftp-drop` shortly afterwards to ensure the deployment was successful.
 If not, you will need to investigate further - view the deployment using a tool like `k9s` or view it on the
 GKE view on the web console UI.

If you update the `sftp-users` secret, you'll need to recreate the
`sftp-drop` pod.

## Adding a new user

### SSH/RSA

To add a new ssh/rsa user, run 

```
./add-sftp-user $newuser::::drop
```

You'll also need to add their public key(s) to a ConfigMap and mount
it into the new user's key directory:
`/home/newuser/.ssh/keys`. Ensure that the end of each public key
contains a newline, or the openssh daemon will not read the keys.

### Username/Password

To add a new username/password user, run

```
./add-sftp-user $username:$password:::drop
```

### IP Addresses

The addition of a new user usually means a new IP address will be
accessing the service. They must be allowed through the firewall by
editing the `loadBalancerSourceRanges` key. Make sure to list the IP
address with subnet (use `/32` if only a single address).

### Getting in - Only in exceptional cases!

#### Directly via k8s
Do not go in to our SFTP server except for on exceptional cases - check with the team first to
 make sure this is the correct course of action.

To access the SFTP server, make sure you have `kubectl` installed and that you have
 run `scripts/auth/gke-cityblock-data`. 

Then run:
```
kubectl config use-context gke_cityblock-data_us-east1-b_airflow
kubectl get pods | grep sftp-drop
```
to get the name of the pod running the SFTP server. Using that name and substituting it for <pod name> run:
```
kubectl exec -it <pod name> -- /bin/bash
```
You can now navigate to wherever you need (for example, partner sftp folders are under /sftp_homes/).

Alternatively you can use [`k9s`](https://k9scli.io/) and shell into the container via that interface.

#### Debugging as a client
You can debug the server by accessing it using the SFTP protocol directly. Do this by getting the private SSH key 
from [our secrets store](https://console.cloud.google.com/security/secret-manager/secret/sftp_key_prod/versions?project=cbh-secrets).

You must also add your IP address to the `LoadBalancer` service in the `all.yml` file. This can be done
 manually and does not require a PR. **Remember to remove it once you are done debugging!!**

Use the `sftp` protocol to connect to the server:
```
KEY_PATH= # path to the SSH key mentioned above
sftp -i $KEY_PATH cityblock_production@sftp.internal.cityblock.engineering
```
