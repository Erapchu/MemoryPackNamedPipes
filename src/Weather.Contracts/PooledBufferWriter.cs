using System.Buffers;

namespace Weather.Contracts;

public sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[]? _buffer;
    private int _written;

    public PooledBufferWriter(int initialCapacity = 256) => _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
    public int WrittenCount => _written;
    public ReadOnlyMemory<byte> WrittenMemory => Buffer.AsMemory(0, _written);

    public void Advance(int count)
    {
        if (count < 0 || _written > Buffer.Length - count) throw new ArgumentOutOfRangeException(nameof(count));
        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0) { EnsureCapacity(sizeHint); return Buffer.AsMemory(_written); }
    public Span<byte> GetSpan(int sizeHint = 0) { EnsureCapacity(sizeHint); return Buffer.AsSpan(_written); }

    public byte[] DetachBuffer()
    {
        var result = Buffer;
        _buffer = null;
        _written = 0;
        return result;
    }

    public void Reset() => _written = 0;
    private byte[] Buffer => _buffer ?? throw new ObjectDisposedException(nameof(PooledBufferWriter));

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint < 0) throw new ArgumentOutOfRangeException(nameof(sizeHint));
        sizeHint = Math.Max(sizeHint, 1);
        if (sizeHint <= Buffer.Length - _written) return;
        var newBuffer = ArrayPool<byte>.Shared.Rent(Math.Max(_written + sizeHint, Buffer.Length * 2));
        Buffer.AsSpan(0, _written).CopyTo(newBuffer);
        ArrayPool<byte>.Shared.Return(Buffer);
        _buffer = newBuffer;
    }

    public void Dispose()
    {
        if (_buffer is null) return;
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = null;
        _written = 0;
    }
}
