NAME=mongodb-grafana-proxy
VERSION=$(shell cat package.json | grep version | head -1 | sed 's/.*: "\([^"]*\)".*/\1/')
PORT=3333
REGISTRY_PREFIX=$(if $(REGISTRY),$(addsuffix /, $(REGISTRY)))

.PHONY: build publish

build:
	docker build -t ${NAME}:${VERSION} .

publish:
	docker tag ${NAME}:${VERSION} ${REGISTRY_PREFIX}${NAME}:${VERSION}
	docker push ${REGISTRY_PREFIX}${NAME}:${VERSION}
