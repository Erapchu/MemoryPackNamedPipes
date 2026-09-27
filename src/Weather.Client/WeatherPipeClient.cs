using System.Diagnostics;
using System.IO.Pipes;
using Weather.Contracts;

namespace Weather.Client;

internal sealed class WeatherPipeClient
{
    private readonly int _id;
    private readonly ClientOptions _options;
    private readonly ClientMetrics _metrics;

    public WeatherPipeClient(int id, ClientOptions options, ClientMetrics metrics)
    { _id = id; _options = options; _metrics = metrics; }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        await using var pipe = new NamedPipeClientStream(".", _options.PipeName, PipeDirection.InOut,
            PipeOptions.Asynchronous | PipeOptions.WriteThrough);
        await pipe.ConnectAsync(10_000, cancellationToken).ConfigureAwait(false);
        _metrics.Connected();
        try
        {
            await SendAsync(pipe, MessageKind.Hello, new ClientHello { ClientId = _id, Location = "Moscow" }, false, cancellationToken).ConfigureAwait(false);
            var sendLoop = SendRequestsAsync(pipe, cancellationToken);
            try { await ReceiveUpdatesAsync(pipe, cancellationToken).ConfigureAwait(false); }
            finally
            {
                try { await sendLoop.ConfigureAwait(false); }
                catch (Exception ex) when (ex is IOException or OperationCanceledException) { }
            }
        }
        catch (Exception ex) when (ex is IOException or EndOfStreamException or OperationCanceledException) { }
        finally { _metrics.Disconnected(); }
    }

    private async Task SendRequestsAsync(Stream pipe, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(_options.RequestIntervalMs));
        long sequence = 0;
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            var request = new WeatherRequest
            {
                Sequence = ++sequence, ClientSentTimestamp = Stopwatch.GetTimestamp(), Location = "Moscow"
            };
            await SendAsync(pipe, MessageKind.WeatherRequest, request, true, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task SendAsync<T>(Stream pipe, MessageKind messageKind, T value, bool record, CancellationToken cancellationToken)
    {
        using var writer = new PooledBufferWriter();
        var started = Stopwatch.GetTimestamp();
        var allocated = GC.GetAllocatedBytesForCurrentThread();
        PayloadSerializer.Serialize(_options.Serializer, writer, value);
        if (record) _metrics.Serialization(Stopwatch.GetElapsedTime(started), GC.GetAllocatedBytesForCurrentThread() - allocated);
        await PipeProtocol.WriteFrameAsync(pipe, messageKind, _options.Serializer, writer.WrittenMemory, cancellationToken).ConfigureAwait(false);
        if (record) _metrics.Sent(PipeProtocol.HeaderSize + writer.WrittenCount);
    }

    private async Task ReceiveUpdatesAsync(Stream pipe, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            using var frame = await PipeProtocol.ReadFrameAsync(pipe, cancellationToken).ConfigureAwait(false);
            if (frame.MessageKind != MessageKind.WeatherUpdate || frame.SerializerKind != _options.Serializer)
                throw new InvalidDataException("Unexpected frame.");
            var started = Stopwatch.GetTimestamp();
            var allocated = GC.GetAllocatedBytesForCurrentThread();
            var update = PayloadSerializer.Deserialize<WeatherUpdate>(_options.Serializer, frame.Payload);
            _metrics.Deserialization(Stopwatch.GetElapsedTime(started), GC.GetAllocatedBytesForCurrentThread() - allocated);
            _metrics.Delivery(Stopwatch.GetElapsedTime(update.ServerSentTimestamp));
            _metrics.Received(PipeProtocol.HeaderSize + frame.Length);
            if (_options.Verbose && _id == 1 && update.Sequence % 100 == 0)
                Console.WriteLine($"[notification] WeatherUpdate #{update.Sequence} deserialized and delivered: {update.TemperatureC:F1} °C, {update.Location}");
        }
    }
}
