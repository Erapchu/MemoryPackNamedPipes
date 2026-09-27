using MemoryPack;
using MessagePack;

namespace Weather.Contracts;

public enum SerializerKind : byte { MemoryPack = 1, MessagePack = 2 }
public enum MessageKind : byte { Hello = 1, WeatherRequest = 2, WeatherUpdate = 3 }

[MemoryPackable]
[MessagePackObject]
public sealed partial class ClientHello
{
    [Key(0)] public int ClientId { get; set; }
    [Key(1)] public string Location { get; set; } = "Moscow";
}

[MemoryPackable]
[MessagePackObject]
public sealed partial class WeatherRequest
{
    [Key(0)] public long Sequence { get; set; }
    [Key(1)] public long ClientSentTimestamp { get; set; }
    [Key(2)] public string Location { get; set; } = "Moscow";
}

[MemoryPackable]
[MessagePackObject]
public sealed partial class WeatherUpdate
{
    [Key(0)] public long Sequence { get; set; }
    [Key(1)] public long ServerSentTimestamp { get; set; }
    [Key(2)] public string Location { get; set; } = "Moscow";
    [Key(3)] public double TemperatureC { get; set; }
    [Key(4)] public double FeelsLikeC { get; set; }
    [Key(5)] public double HumidityPercent { get; set; }
    [Key(6)] public double PressureHpa { get; set; }
    [Key(7)] public double WindSpeedMps { get; set; }
    [Key(8)] public int ConditionCode { get; set; }
}
