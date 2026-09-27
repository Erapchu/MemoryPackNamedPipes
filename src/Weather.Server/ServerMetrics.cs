using Weather.Contracts;

namespace Weather.Server;

internal sealed class ServerMetrics
{
    private long _accepted, _connected, _enqueued, _dropped, _receivedRequests, _bytesSent;
    private long _memoryPackAllocated, _messagePackAllocated, _requestAllocated;
    private readonly LatencySummary _memoryPackSerialization = new();
    private readonly LatencySummary _messagePackSerialization = new();
    private readonly LatencySummary _requestDeserialization = new();

    public void Connected() { Interlocked.Increment(ref _accepted); Interlocked.Increment(ref _connected); }
    public void Disconnected() => Interlocked.Decrement(ref _connected);
    public void Enqueued() => Interlocked.Increment(ref _enqueued);
    public void Dropped() => Interlocked.Increment(ref _dropped);
    public void RequestReceived() => Interlocked.Increment(ref _receivedRequests);
    public void BytesSent(int count) => Interlocked.Add(ref _bytesSent, count);
    public void Serialization(SerializerKind kind, TimeSpan elapsed, long allocatedBytes)
    {
        (kind == SerializerKind.MemoryPack ? _memoryPackSerialization : _messagePackSerialization).Record(elapsed);
        Interlocked.Add(ref kind == SerializerKind.MemoryPack ? ref _memoryPackAllocated : ref _messagePackAllocated, allocatedBytes);
    }
    public void RequestDeserialization(TimeSpan elapsed, long allocatedBytes)
    {
        _requestDeserialization.Record(elapsed);
        Interlocked.Add(ref _requestAllocated, allocatedBytes);
    }

    public string Format()
    {
        var mem = _memoryPackSerialization.GetSnapshot();
        var msg = _messagePackSerialization.GetSnapshot();
        var deser = _requestDeserialization.GetSnapshot();
        return $"clients={Interlocked.Read(ref _connected)} accepted={Interlocked.Read(ref _accepted)} " +
               $"enqueued={Interlocked.Read(ref _enqueued)} dropped={Interlocked.Read(ref _dropped)} " +
               $"requests={Interlocked.Read(ref _receivedRequests)} bytes={Interlocked.Read(ref _bytesSent)} | " +
               $"serialize MemoryPack mean/p95={mem.MeanUs:F2}/{mem.P95Us:F2} us {PerOp(_memoryPackAllocated, mem.Count):F1} B/op, " +
               $"MessagePack={msg.MeanUs:F2}/{msg.P95Us:F2} us {PerOp(_messagePackAllocated, msg.Count):F1} B/op | " +
               $"request deserialize mean/p95={deser.MeanUs:F2}/{deser.P95Us:F2} us {PerOp(_requestAllocated, deser.Count):F1} B/op";
    }

    private static double PerOp(long allocated, long count) => count == 0 ? 0 : (double)Interlocked.Read(ref allocated) / count;
}
