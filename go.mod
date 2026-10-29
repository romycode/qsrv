module github.com/romycode/qsrv

go 1.27.0

require (
	github.com/mattn/go-sqlite3 v1.14.52
	github.com/ncruces/go-sqlite3 v0.30.5
	github.com/tetratelabs/wazero v1.11.0
)

require (
	github.com/ncruces/julianday v1.0.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
)

// Official GitHub mirrors of golang.org/x (golang.org was unreachable when
// this was built). Safe to delete these lines and run `go mod tidy`.
replace (
	golang.org/x/crypto => github.com/golang/crypto v0.47.0
	golang.org/x/sync => github.com/golang/sync v0.19.0
	golang.org/x/sys => github.com/golang/sys v0.40.0
	golang.org/x/text => github.com/golang/text v0.33.0
)
