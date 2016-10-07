
clean:
	rm -f coverage.out
	go clean -i ./...

build:
	go build

deps:
	go get github/tools/godep
	godep restore

test:
	go test -cover .

cov:
	go test -coverprofile=coverage.out .
	go tool cover -html=coverage.out

