module weight_conflict_demo

go 1.24.0

replace github.com/Fabricates/Matcher => ../..

require github.com/Fabricates/Matcher v0.0.0-00010101000000-000000000000

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/redis/go-redis/v9 v9.5.1 // indirect
)
