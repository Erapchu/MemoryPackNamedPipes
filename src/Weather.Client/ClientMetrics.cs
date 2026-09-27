using Weather.Contracts;

namespace Weather.Client;

internal sealed class ClientMetrics
{
    private long _connected, _received, _sent, _bytesReceived, _bytesSent;
    private long _serializationAllocated, _deserializationAllocated;
    private readonly LatencySummary _serialization = new();
    private readonly LatencySummary _deserialization = new();
    private readonly LatencySummary _delivery = new();

    public void Connected() => Interlocked.Increment(ref _connected);
    public void Disconnected() => Interlocked.Decrement(ref _connected);
    public void Received(int bytes) { Interlocked.Increment(ref _received); Interlocked.Add(ref _bytesReceived, bytes); }
    public void Sent(int bytes) { Interlocked.Increment(ref _sent); Interlocked.Add(ref _bytesSent, bytes); }
    public void Serialization(TimeSpan elapsed, long allocatedBytes)
    { _serialization.Record(elapsed); Interlocked.Add(ref _serializationAllocated, allocatedBytes); }
    public void Deserialization(TimeSpan elapsed, long allocatedBytes)
    { _deserialization.Record(elapsed); Interlocked.Add(ref _deserializationAllocated, allocatedBytes); }
    public void Delivery(TimeSpan elapsed) => _delivery.Record(elapsed);

    public string Format(SerializerKind serializer)
    {
        var ser = _serialization.GetSnapshot();
        var deser = _deserialization.GetSnapshot();
        var delivery = _delivery.GetSnapshot();
        return $"{serializer}: clients={Interlocked.Read(ref _connected)} received={Interlocked.Read(ref _received)} " +
               $"sent={Interlocked.Read(ref _sent)} bytes-in/out={Interlocked.Read(ref _bytesReceived)}/{Interlocked.Read(ref _bytesSent)} | " +
               $"serialize mean/p95/p99={ser.MeanUs:F2}/{ser.P95Us:F2}/{ser.P99Us:F2} us {PerOp(_serializationAllocated, ser.Count):F1} B/op | " +
               $"deserialize={deser.MeanUs:F2}/{deser.P95Us:F2}/{deser.P99Us:F2} us {PerOp(_deserializationAllocated, deser.Count):F1} B/op | " +
               $"delivery={delivery.MeanUs:F2}/{delivery.P95Us:F2}/{delivery.P99Us:F2} us";
    }

    private static double PerOp(long allocated, long count) => count == 0 ? 0 : (double)Interlocked.Read(ref allocated) / count;
}
