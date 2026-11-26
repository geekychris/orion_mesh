module github.com/geekychris/orion_mesh/examples/16-go-client

go 1.26.0

require github.com/geekychris/orion_mesh/clients/go v0.0.0

require (
	github.com/klauspost/compress v1.20.5 // indirect
	github.com/nats-io/nats.go v1.37.0 // indirect
	github.com/nats-io/nkeys v0.4.7 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.59.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/geekychris/orion_mesh/clients/go => ../../clients/go
