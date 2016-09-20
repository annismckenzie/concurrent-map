.PHONY: test-coverage bench

test-coverage:
	go test -covermode=count -coverprofile=profile.cov
	go tool cover --func=profile.cov | grep 'total:'
	go tool cover -html=profile.cov -o coverage.html

bench:
	go test -run=XXX -bench=. -benchmem
