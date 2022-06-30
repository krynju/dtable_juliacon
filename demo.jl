using Pkg
Pkg.activate(".")
Pkg.add(url="https://github.com/JuliaParallel/DTables.jl")

using DTables
using RDatasets
using Statistics
using StatsBase
using CategoricalArrays
using OnlineStats

@info Threads.nthreads()

# N = 10_000_000
# csize = 100_000
N = 10_000_0
csize = 100_0


# source DataFrame
df = let
    d = dataset("mlmRev", "Chem97")[:, Not([5,6])]
    cs = propertynames(d)[1:3]
    select(d,
        cs .=> (x -> parse.(Int16, unwrap.(x))) .=> cs,
        :
    )
end

# Creating a DTable from `df`
dt = DTable(df[sample(1:nrow(df), N), :], csize)

# Underlying type is `DataFrame`, because we used a `DataFrame` to construct it
# You can change the tabletype on DTable construction though
fetch(first(dt.chunks))

# Getting back the full table into any format is simple
fetch(dt)
fetch(dt, DataFrame)
fetch(dt, NamedTuple)

# Operations

# Step 1: reduce - let's get the extremas of the Score column
r = reduce(fit!, dt, cols=[:Score], init=Extrema())
e = fetch(r)

_max = e.Score.max
_min = e.Score.min

# Step 2: mapping the `Score` (0-10) to a Grade (2-5)
# mapping function needs to return a tuple

# y = ax + b
a = (5 - 2) / (_max - _min)
b = 5 - _max * a

m = map(
    row -> (;
        row...,
        Grade=row.Score * a + b,
    ),
    dt
)
fetch(m)

# Step 3: filter passing grades
f = filter(row -> row.Grade >= 3, m)
length(f) / length(m) * 100

# Step 4: stats (GCSEScore inside grade groupd)
# let's use mapreduce

assign_group(x) = begin
    2 <= x < 3 && return "2-3"
    3 <= x < 4 && return "3-4"
    4 <= x <= 5 && return "4-5"
end

stats = mapreduce(
    row -> (assign_group(row.Grade), row.GCSEScore),
    fit!,
    m,
    init=GroupBy(String, Mean())
) |> fetch

# or simpler

stats = mapreduce(
    row -> (row.Grade, row.GCSEScore),
    fit!,
    m,
    init=GroupBy(Float64, Mean())
) |> fetch

using Plots
bar(
    stats.value.keys,
    getproperty.(stats.value.vals, :μ),
    xticks=stats.value.keys,
    ylabel="avg GCSEScore",
    xlabel="grade",
    legend=:none,
)


# Step 5: groupby
# by Lea - Local Education Authority - some area with schools
gt = DTables.groupby(m, :Lea, chunksize=csize)
gt.index

r = reduce(fit!, gt, cols=[:Grade], init=Mean())
r2 = fetch(r)
bar(
    r2.Lea,
    getproperty.(r2.result_Grade, :μ),
    ylabel="mean grade",
    xlabel="lea"
)

# Extra: experimental DataFrames-like select

DTables.select(
    dt,
    :Lea => :Lea2,
    :Score,
    :Score => ByRow(x-> x * a + b) => :Grade, # parallel,
    :Score => mean, # not parallel
    [] => ByRow(() -> Threads.threadid()) => :threadid, # parallel
) |> fetch
    