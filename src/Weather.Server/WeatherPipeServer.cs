using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Pipes;
using Weather.Contracts;

namespace Weather.Server;

internal sealed class WeatherPipeServer
{
    private readonly string _pipeName;
    private readonly TimeSpan _publishInterval;
    private readonly int _queueCapacity;
    private readonly ConcurrentDictionary<long, ClientSession> _sessions = new();
    private readonly ServerMetrics _metrics = new();
    private long _nextSessionId, _sequence;

    public WeatherPipeServer(string pipeName, TimeSpan publishInterval, int queueCapacity)
    { _pipeName = pipeName; _publishInterval = publishInterval; _queueCapacity = queueCapacity; }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine($"Server listening on pipe '{_pipeName}', interval={_publishInterval.TotalMilliseconds:F0} ms.");
        await Task.WhenAll(AcceptLoopAsync(cancellationToken), PublishLoopAsync(cancellationToken), ReportLoopAsync(cancellationToken)).ConfigureAwait(false);
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var pipe = new NamedPipeServerStream(_pipeName, PipeDirection.InOut,
                NamedPipeServerStream.MaxAllowedServerInstances, PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous | PipeOptions.WriteThrough);
            try
            {
                await pipe.WaitForConnectionAsync(cancellationToken).ConfigureAwait(false);
                _ = HandleConnectionAsync(pipe, cancellationToken);
            }
            catch { await pipe.DisposeAsync().ConfigureAwait(false); throw; }
        }
    }

    private async Task HandleConnectionAsync(NamedPipeServerStream pipe, CancellationToken cancellationToken)
    {
        ClientSession? session = null;
        try
        {
            using var helloFrame = await PipeProtocol.ReadFrameAsync(pipe, cancellationToken).ConfigureAwait(false);
            if (helloFrame.MessageKind != MessageKind.Hello) throw new InvalidDataException("The first frame must be Hello.");
            _ = PayloadSerializer.Deserialize<ClientHello>(helloFrame.SerializerKind, helloFrame.Payload);
            var id = Interlocked.Increment(ref _nextSessionId);
            session = new ClientSession(id, pipe, helloFrame.SerializerKind, _metrics, _queueCapacity);
            _sessions[id] = session;
            _metrics.Connected();
            await session.RunAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or EndOfStreamException or InvalidDataException or OperationCanceledException) { }
        finally
        {
            if (session is not null)
            {
                _sessions.TryRemove(session.Id, out _);
                _metrics.Disconnected();
                await session.DisposeAsync().ConfigureAwait(false);
            }
            else await pipe.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task PublishLoopAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_publishInterval);
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            var update = CreateUpdate(Interlocked.Increment(ref _sequence));
            foreach (var serializer in Enum.GetValues<SerializerKind>())
            {
                if (!_sessions.Values.Any(session => session.SerializerKind == serializer)) continue;
                using var writer = new PooledBufferWriter();
                var started = Stopwatch.GetTimestamp();
                var allocated = GC.GetAllocatedBytesForCurrentThread();
                PayloadSerializer.Serialize(serializer, writer, update);
                _metrics.Serialization(serializer, Stopwatch.GetElapsedTime(started), GC.GetAllocatedBytesForCurrentThread() - allocated);
                var length = writer.WrittenCount;
                var shared = new SharedFrame(MessageKind.WeatherUpdate, serializer, writer.DetachBuffer(), length);
                try
                {
                    foreach (var session in _sessions.Values)
                        if (session.SerializerKind == serializer) session.TryEnqueue(shared);
                }
                finally { shared.Release(); }
            }
        }
    }

    private static WeatherUpdate CreateUpdate(long sequence)
    {
        var wave = Math.Sin(sequence / 25d);
        return new WeatherUpdate
        {
            Sequence = sequence, ServerSentTimestamp = Stopwatch.GetTimestamp(), Location = "Moscow",
            TemperatureC = 12 + wave * 8, FeelsLikeC = 10 + wave * 9,
            HumidityPercent = 55 - wave * 15, PressureHpa = 1013 + wave * 7,
            WindSpeedMps = 2.5 + Math.Abs(wave) * 4, ConditionCode = 2
        };
    }

    private async Task ReportLoopAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            Console.WriteLine($"[server] {_metrics.Format()}");
    }
}
