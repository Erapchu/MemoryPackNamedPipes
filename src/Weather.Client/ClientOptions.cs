using Weather.Contracts;

namespace Weather.Client;

internal sealed record ClientOptions(string PipeName, SerializerKind Serializer, int ClientCount,
    int DurationSeconds, int RequestIntervalMs, bool Verbose)
{
    public static ClientOptions Parse(string[] args)
    {
        if (args.Contains("--help"))
        {
            Console.WriteLine("Weather.Client [--pipe weather-bench] [--serializer memorypack|messagepack] [--clients 1] [--duration 15] [--request-ms 100] [--quiet]");
            Environment.Exit(0);
        }
        var serializer = Value(args, "--serializer", "memorypack").Equals("messagepack", StringComparison.OrdinalIgnoreCase)
            ? SerializerKind.MessagePack : SerializerKind.MemoryPack;
        return new ClientOptions(Value(args, "--pipe", "weather-bench"), serializer,
            IntValue(args, "--clients", 1), IntValue(args, "--duration", 15),
            IntValue(args, "--request-ms", 100), !args.Contains("--quiet"));
    }

    private static string Value(string[] args, string name, string fallback)
    { var index = Array.IndexOf(args, name); return index >= 0 && index + 1 < args.Length ? args[index + 1] : fallback; }
    private static int IntValue(string[] args, string name, int fallback) =>
        int.TryParse(Value(args, name, fallback.ToString()), out var value) && value > 0 ? value : fallback;
}
