set -x

# Build
docker build . -t stephben/fetch-with-prefect:latest

# Publish to hub.docker.com
docker login --username stephben
docker push stephben/fetch-with-prefect:latest