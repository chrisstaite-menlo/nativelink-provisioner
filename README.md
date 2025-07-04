Nativelink provisioner
======================

This repository contains a simple tool that monitors a nativelink deployment
to determine whether additional workers are required and starts Pods for those
workers.  It is meant to augment the Nativelink helm chart to override the
worker provisioner that comes with it for specifically Chromium based build
clusters.

You can build the Docker image using the following:
```sh
IMAGE_INFO=$(docker load < $(nix-build -A image default.nix) | tail -n1)
SOURCE_IMAGE=$(echo "$IMAGE_INFO" | sed 's/Loaded image: //')
```

An example Kubernetes deployment is available in the examples directory.

Note that you will need to update the project ID for the prometheus frontend
to match your GCP project ID and also update the image in the deployment to
match where the built image above is.
