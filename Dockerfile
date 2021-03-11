FROM golang:1.15.9 AS build

ENV GOPROXY="https://goproxy.cn"

RUN mkdir -p /opt/spot/build

COPY . /root/spot/build
WORKDIR /root/spot/build

RUN set -ex && \
    echo "Asia/Shanghai" >/etc/timezone && \
	./build.sh /tmp/build && \
	mv start.sh /opt/spot/ && \
	mv filebeat/filebeat /opt/spot/ && \
	mv filebeat/conf /opt/spot/conf && \
	mv VERSION /opt/spot/

FROM registry.cn-hangzhou.aliyuncs.com/terminus/terminus-centos:base

MAINTAINER terminus-spot-team

WORKDIR /app

COPY --from=build /opt/spot/start.sh /app/
COPY --from=build /opt/spot/filebeat /app/
COPY --from=build /opt/spot/conf/* /app/conf/
COPY --from=build /opt/spot/VERSION /app/

CMD "./start.sh"
