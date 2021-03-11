FROM golang:1.14.15 AS build

ENV GOPROXY="https://goproxy.cn"

RUN mkdir -p /opt/spot/build

COPY . /root/spot/build
WORKDIR /root/spot/build

RUN set -ex && \
    echo "Asia/Shanghai" >/etc/timezone && \
	./build.sh /tmp/build && \
	cp start.sh /opt/spot/ && \
	cp filebeat/filebeat /opt/spot/ && \
	cp -r filebeat/conf /opt/spot/ && \
	cp VERSION /opt/spot/

FROM registry.cn-hangzhou.aliyuncs.com/terminus/terminus-centos:base

MAINTAINER terminus-spot-team

WORKDIR /app

COPY --from=build /opt/spot/start.sh /app/
COPY --from=build /opt/spot/filebeat /app/
COPY --from=build /opt/spot/conf /app/
COPY --from=build /opt/spot/VERSION /app/

CMD "./start.sh"
