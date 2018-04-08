FROM golang:1.10 AS build
WORKDIR /go/src/github.com/hpidcock/gke-scale-disk-sidecar
ADD main.go .
RUN go get
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o gke-scale-disk-sidecar .

FROM alpine
RUN apk add --no-cache ca-certificates
WORKDIR /root/
COPY --from=build /go/src/github.com/hpidcock/gke-scale-disk-sidecar/gke-scale-disk-sidecar .
ENTRYPOINT [ "/root/gke-scale-disk-sidecar" ]
