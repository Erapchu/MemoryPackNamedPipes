using MemoryPack;
using MessagePack;

namespace Weather.Contracts;

public static class PayloadSerializer
{
    private static readonly MessagePackSerializerOptions MessagePackOptions =
        MessagePackSerializerOptions.Standard.WithSecurity(MessagePackSecurity.UntrustedData);

    public static void Serialize<T>(SerializerKind kind, PooledBufferWriter writer, T value)
    {
        writer.Reset();
        switch (kind)
        {
            case SerializerKind.MemoryPack:
                MemoryPackSerializer.Serialize(writer, value);
                break;
            case SerializerKind.MessagePack:
                MessagePackSerializer.Serialize(writer, value, MessagePackOptions);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    public static T Deserialize<T>(SerializerKind kind, ReadOnlyMemory<byte> payload) => kind switch
    {
        SerializerKind.MemoryPack => MemoryPackSerializer.Deserialize<T>(payload.Span)
            ?? throw new InvalidDataException("MemoryPack returned null."),
        SerializerKind.MessagePack => MessagePackSerializer.Deserialize<T>(payload, MessagePackOptions),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };
}
