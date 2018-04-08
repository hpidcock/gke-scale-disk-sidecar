FROM golang:1.10 AS build
WORKDIR /go/src/github.com/hpidcock/gke-scale-disk-sidecar
RUN go get "github.com/hashicorp/go-multierror" \
"golang.org/x/oauth2/google" \
"google.golang.org/api/compute/v1" \
"k8s.io/api/core/v1" \
"k8s.io/apimachinery/pkg/apis/meta/v1" \
"k8s.io/client-go/kubernetes" \
"k8s.io/client-go/rest"
ADD main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o gke-scale-disk-sidecar .

FROM alpine
RUN apk add --no-cache ca-certificates
WORKDIR /root/
COPY --from=build /go/src/github.com/hpidcock/gke-scale-disk-sidecar/gke-scale-disk-sidecar .
ENTRYPOINT [ "/root/gke-scale-disk-sidecar" ]
