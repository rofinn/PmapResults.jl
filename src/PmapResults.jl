module PmapResults

using ResultTypes
using Memento

import Base: Callable
import Base.Distributed:
    pmap,
    wrap_on_error,
    wrap_retry,
    wrap_batch,
    AbstractWorkerPool,
    BatchProcessingError


const LOGGER = getlogger(current_module())
__init__() = Memento.register(LOGGER)


# This is pretty much copied from base in order to wrap the remote function.
function pmap(
    p::AbstractWorkerPool, f::Callable, T::Type{<:Result}, c;
    distributed=true, batch_size=1, on_error=nothing, retry_delays=[], retry_check=nothing
)
    f_orig = f
    # Don't do remote calls if there are no workers.
    if (length(p) == 0) || (length(p) == 1 && fetch(p.channel) == myid())
        distributed = false
    end

    # Don't do batching if not doing remote calls.
    if !distributed
        batch_size = 1
    end

    # If not batching, do simple remote call.
    if batch_size == 1
        if on_error !== nothing
            f = wrap_on_error(f, on_error)
        end

        if distributed
            f = remote(p, f)
        end

        if length(retry_delays) > 0
            f = wrap_retry(f, retry_delays, retry_check)
        end

        f = wrap_result(T, f)

        return asyncmap(f, c; ntasks=()->nworkers(p))
    else
        # During batch processing, We need to ensure that if on_error is set, it is called
        # for each element in error, and that we return as many elements as the original list.
        # retry, if set, has to be called element wise and we will do a best-effort
        # to ensure that we do not call mapped function on the same element more than length(retry_delays).
        # This guarantee is not possible in case of worker death / network errors, wherein
        # we will retry the entire batch on a new worker.

        handle_errors = ((on_error !== nothing) || (length(retry_delays) > 0))

        # Unlike the non-batch case, in batch mode, we trap all errors and the on_error hook (if present)
        # is processed later in non-batch mode.
        if handle_errors
            f = wrap_on_error(f, (x,e)->BatchProcessingError(x,e); capture_data=true)
        end

        f = wrap_batch(f, p, handle_errors)
        results = asyncmap(f, c; ntasks=()->nworkers(p), batch_size=batch_size)

        # process errors if any.
        if handle_errors
            _process_batch_errors!(p, f_orig, results, on_error, retry_delays, retry_check)
        end

        return map(results) do x
            result = convert(T, x)::T
            if ResultTypes.iserror(result)
                warn(LOGGER, e)
            end
        end
    end
end

pmap(p::AbstractWorkerPool, f::Callable, T::Type{<:Result}, c1, c...; kwargs...) =
    pmap(p, a->f(a...), T, zip(c1, c...); kwargs...)

pmap(f::Callable, T::Type{<:Result}, c; kwargs...) =
    pmap(default_worker_pool(), f, T, c; kwargs...)

pmap(f::Callable, T::Type{<:Result}, c1, c...; kwargs...) =
    pmap(a->f(a...), T, zip(c1, c...); kwargs...)

function wrap_result(T::Type{<:Result}, f::Callable)
    return x -> begin
        result = try
            f(x)
        catch e
            warn(LOGGER, e)
            e
        end

        convert(T, result)::T
    end
end

function _process_batch_errors!(p, f, results, on_error, retry_delays, retry_check)
    # Handle all the ones in error in another pmap, with batch size set to 1
    reprocess = []
    for (idx, v) in enumerate(results)
        if isa(v, BatchProcessingError)
            push!(reprocess, (idx,v))
        end
    end

    if length(reprocess) > 0
        errors = [x[2] for x in reprocess]
        exceptions = [x.ex for x in errors]
        state = start(retry_delays)
        if (length(retry_delays) > 0) &&
                (retry_check==nothing || all([retry_check(state,ex)[2] for ex in exceptions]))
            # BatchProcessingError.data is a tuple of original args
            error_processed = pmap(p, x->f(x...), [x.data for x in errors];
                    on_error = on_error, retry_delays = collect(retry_delays)[2:end], retry_check = retry_check)
        elseif on_error !== nothing
            error_processed = map(on_error, exceptions)
        else
            error_processed = exceptions
        end

        for (idx, v) in enumerate(error_processed)
            results[reprocess[idx][1]] = v
        end
    end
    nothing
end

end  # module
