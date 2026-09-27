using System.Buffers;
using Weather.Contracts;

namespace Weather.Server;

internal sealed class SharedFrame
{
    private byte[]? _buffer;
    private int _references = 1;

    public SharedFrame(MessageKind messageKind, SerializerKind serializerKind, byte[] buffer, int length)
    {
        MessageKind = messageKind;
        SerializerKind = serializerKind;
        _buffer = buffer;
        Length = length;
    }

    public MessageKind MessageKind { get; }
    public SerializerKind SerializerKind { get; }
    public int Length { get; }
    public ReadOnlyMemory<byte> Payload => (_buffer ?? throw new ObjectDisposedException(nameof(SharedFrame))).AsMemory(0, Length);
    public void AddReference() => Interlocked.Increment(ref _references);

    public void Release()
    {
        if (Interlocked.Decrement(ref _references) != 0) return;
        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer is not null) ArrayPool<byte>.Shared.Return(buffer);
    }
}
