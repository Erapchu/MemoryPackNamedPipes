using System.Diagnostics;
using System.IO.Pipes;
using System.Threading.Channels;
using Weather.Contracts;

namespace Weather.Server;

internal sealed class ClientSession : IAsyncDisposable
{
    private readonly NamedPipeServerStream _pipe;
    private readonly ServerMetrics _metrics;
    private readonly Channel<SharedFrame> _outbound;
    private readonly CancellationTokenSource _stop = new();
    private int _closed;

    public ClientSession(long id, NamedPipeServerStream pipe, SerializerKind serializerKind, ServerMetrics metrics, int queueCapacity)
    {
        Id = id;
        _pipe = pipe;
        SerializerKind = serializerKind;
        _metrics = metrics;
        _outbound = Channel.CreateBounded<SharedFrame>(new BoundedChannelOptions(queueCapacity)
        {
            SingleReader = true, SingleWriter = false, FullMode = BoundedChannelFullMode.Wait
        });
    }

    public long Id { get; }
    public SerializerKind SerializerKind { get; }

    public bool TryEnqueue(SharedFrame frame)
    {
        frame.AddReference();
        if (_outbound.Writer.TryWrite(frame)) { _metrics.Enqueued(); return true; }
        frame.Release();
        _metrics.Dropped();
        return false;
    }

    public Task RunAsync(CancellationToken serverToken) => RunCoreAsync(serverToken);

    private async Task RunCoreAsync(CancellationToken serverToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(serverToken, _stop.Token);
        var writer = WriteLoopAsync(linked.Token);
        try { await ReadLoopAsync(linked.Token).ConfigureAwait(false); }
        catch (Exception ex) when (ex is EndOfStreamException or IOException or OperationCanceledException) { }
        finally
        {
            await _stop.CancelAsync().ConfigureAwait(false);
            _outbound.Writer.TryComplete();
            try { await writer.ConfigureAwait(false); }
            catch (Exception ex) when (ex is IOException or OperationCanceledException) { }
        }
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            using var frame = await PipeProtocol.ReadFrameAsync(_pipe, cancellationToken).ConfigureAwait(false);
            if (frame.MessageKind != MessageKind.WeatherRequest || frame.SerializerKind != SerializerKind)
                throw new InvalidDataException("Unexpected frame.");
            var started = Stopwatch.GetTimestamp();
            var allocated = GC.GetAllocatedBytesForCurrentThread();
            _ = PayloadSerializer.Deserialize<WeatherRequest>(SerializerKind, frame.Payload);
            _metrics.RequestDeserialization(Stopwatch.GetElapsedTime(started), GC.GetAllocatedBytesForCurrentThread() - allocated);
            _metrics.RequestReceived();
        }
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var frame in _outbound.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                try
                {
                    await PipeProtocol.WriteFrameAsync(_pipe, frame.MessageKind, frame.SerializerKind, frame.Payload, cancellationToken).ConfigureAwait(false);
                    _metrics.BytesSent(PipeProtocol.HeaderSize + frame.Length);
                }
                finally { frame.Release(); }
            }
        }
        finally { while (_outbound.Reader.TryRead(out var frame)) frame.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0) return;
        await _stop.CancelAsync().ConfigureAwait(false);
        _outbound.Writer.TryComplete();
        await _pipe.DisposeAsync().ConfigureAwait(false);
        _stop.Dispose();
    }
}
