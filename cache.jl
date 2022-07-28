ENV["JULIA_MEMPOOL_EXPERIMENTAL_FANCY_ALLOCATOR"] = "true"
ENV["JULIA_MEMPOOL_EXPERIMENTAL_MEMORY_BOUND"] = string(2 * (2^30)) # 2GB
ENV["JULIA_MEMPOOL_EXPERIMENTAL_DISK_CACHE"] = "C:\\Users\\krynjupc\\.mempool\\demo_session_$(rand(Int))"

function view_cache()
    !isdir(ENV["JULIA_MEMPOOL_EXPERIMENTAL_DISK_CACHE"]) && return []
    map(
        x -> (basename(x), round(filesize(x) / 2^20, digits=2)),
        readdir(ENV["JULIA_MEMPOOL_EXPERIMENTAL_DISK_CACHE"], join=true)
    )
end

using DTables
using MemPool
using Dagger

N1 = 2^27 # 1GB
d = DTable((a=rand(Int, N1),), N1 ÷ 100)
map(x -> (r=x.a + 1,), d) |> fetch
MemPool.GLOBAL_DEVICE[]
view_cache()

N2 = 3 * 2^27 # 3GB
d = DTable((a=rand(Int, N2),), N2 ÷ 100)
map(x -> (r=x.a + 1,), d) |> fetch
MemPool.GLOBAL_DEVICE[]
GC.gc()
view_cache()
