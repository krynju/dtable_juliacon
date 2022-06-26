using Pkg
Pkg.activate(".")
Pkg.add(url="https://github.com/JuliaParallel/DTables.jl")
Pkg.add(["RDatasets", "OnlineStats"])

using DTables
using RDatasets
using Statistics

@info Threads.nthreads()

df = dataset("mlmRev", "Chem97")[:, Not([5,6])]
df = select(df,
    propertynames(df)[1:3] .=> (x -> parse.(Int16, unwrap.(x))) .=> propertynames(df)[1:3],
    :
)


dt = DTable(df[sample(1:nrow(df), 10_000_000), :], 100_000)


fetch(dt, DataFrame)

gt = DTables.groupby(dt, :Lea)
