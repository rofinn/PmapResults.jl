using Documenter, PmapResults

makedocs(
    modules=[PmapResults],
    format=:html,
    pages=[
        "Home" => "index.md",
    ],
    repo="https://github.com/rofinn/PmapResults.jl/blob/{commit}{path}#L{line}",
    sitename="PmapResults.jl",
    authors="Rory Finnegan",
    assets=[],
)

deploydocs(
    repo = "github.com/rofinn/PmapResults.jl.git",
    target = "build",
    deps = nothing,
    make = nothing,
)
