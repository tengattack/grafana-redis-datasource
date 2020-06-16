#!/bin/sh

GRAFANA_VERSION=6.7.4

if [ ! -d grafana ]; then
  wget https://dl.grafana.com/oss/release/grafana-${GRAFANA_VERSION}.darwin-amd64.tar.gz
  tar -zxvf grafana-${GRAFANA_VERSION}.darwin-amd64.tar.gz
  mv grafana-${GRAFANA_VERSION} grafana
fi

# copy plugin assets
rm -rf grafana/plugins/grafana-redis-datasource
mkdir -p grafana/plugins/grafana-redis-datasource
cp -r ../dist grafana/plugins/grafana-redis-datasource

./grafana/bin/grafana-server -config ./grafana.ini -homepath ./grafana
