using PmapResults
using Base.Test

using ResultTypes

addprocs(5)
@everywhere using PmapResults

try
    # Write your own tests here.
    results = pmap(Result{Int, Exception}, 1:10; on_error=e->rethrow(e), retry_delays=ExponentialBackOff(n=3)) do x
        if iseven(x)
            throw(ErrorException("foo"))
        else
            return x
        end
    end

    for i in 1:10
        if iseven(i)
            @test ResultTypes.iserror(results[i])
        else
            @test ResultTypes.unwrap(results[i]) == i
        end
    end
finally
    rmprocs(5)
end
