using System.Buffers;
using System.Buffers.Binary;

namespace Weather.Contracts;

public readonly record struct ReceivedFrame(MessageKind MessageKind, SerializerKind SerializerKind, byte[] Buffer, int Length) : IDisposable
{
    public ReadOnlyMemory<byte> Payload => Buffer.AsMemory(0, Length);
    public void Dispose() => ArrayPool<byte>.Shared.Return(Buffer);
}

public static class PipeProtocol
{
    public const int HeaderSize = 6;
    public const int MaxPayloadSize = 1024 * 1024;

    public static async ValueTask WriteFrameAsync(Stream stream, MessageKind messageKind, SerializerKind serializerKind,
        ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var header = ArrayPool<byte>.Shared.Rent(HeaderSize);
        try
        {
            header[0] = (byte)messageKind;
            header[1] = (byte)serializerKind;
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(2, 4), payload.Length);
            await stream.WriteAsync(header.AsMemory(0, HeaderSize), cancellationToken).ConfigureAwait(false);
            await stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
        }
        finally { ArrayPool<byte>.Shared.Return(header); }
    }

    public static async ValueTask<ReceivedFrame> ReadFrameAsync(Stream stream, CancellationToken cancellationToken)
    {
        var header = ArrayPool<byte>.Shared.Rent(HeaderSize);
        try
        {
            await ReadExactlyAsync(stream, header.AsMemory(0, HeaderSize), cancellationToken).ConfigureAwait(false);
            var length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(2, 4));
            if (length is < 0 or > MaxPayloadSize) throw new InvalidDataException($"Invalid payload length: {length}.");
            var payload = ArrayPool<byte>.Shared.Rent(Math.Max(length, 1));
            try
            {
                await ReadExactlyAsync(stream, payload.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
                return new ReceivedFrame((MessageKind)header[0], (SerializerKind)header[1], payload, length);
            }
            catch { ArrayPool<byte>.Shared.Return(payload); throw; }
        }
        finally { ArrayPool<byte>.Shared.Return(header); }
    }

    private static async ValueTask ReadExactlyAsync(Stream stream, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var read = 0;
        while (read < buffer.Length)
        {
            var count = await stream.ReadAsync(buffer[read..], cancellationToken).ConfigureAwait(false);
            if (count == 0) throw new EndOfStreamException("The pipe was closed.");
            read += count;
        }
    }
}
