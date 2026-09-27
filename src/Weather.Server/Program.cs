using Weather.Server;

var options = ServerOptions.Parse(args);
using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) => { eventArgs.Cancel = true; shutdown.Cancel(); };

try
{
    await new WeatherPipeServer(options.PipeName, TimeSpan.FromMilliseconds(options.IntervalMs), options.QueueCapacity)
        .RunAsync(shutdown.Token);
}
catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { Console.WriteLine("Server stopped."); }

internal sealed record ServerOptions(string PipeName, int IntervalMs, int QueueCapacity)
{
    public static ServerOptions Parse(string[] args)
    {
        if (args.Contains("--help"))
        {
            Console.WriteLine("Weather.Server [--pipe weather-bench] [--interval-ms 10] [--queue 256]");
            Environment.Exit(0);
        }
        return new ServerOptions(Value(args, "--pipe", "weather-bench"), IntValue(args, "--interval-ms", 10), IntValue(args, "--queue", 256));
    }

    private static string Value(string[] args, string name, string fallback)
    { var index = Array.IndexOf(args, name); return index >= 0 && index + 1 < args.Length ? args[index + 1] : fallback; }
    private static int IntValue(string[] args, string name, int fallback) =>
        int.TryParse(Value(args, name, fallback.ToString()), out var value) && value > 0 ? value : fallback;
}
