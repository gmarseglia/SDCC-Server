FROM golang:1.21.6

WORKDIR /go/src/server

COPY /server .

RUN go build -v -o /usr/local/bin/server ./server.go

EXPOSE 55555
EXPOSE 55556

ENTRYPOINT [ "server" ]