using Weather.Client;

var options = ClientOptions.Parse(args);
var metrics = new ClientMetrics();
using var duration = new CancellationTokenSource(TimeSpan.FromSeconds(options.DurationSeconds));
using var shutdown = CancellationTokenSource.CreateLinkedTokenSource(duration.Token);
Console.CancelKeyPress += (_, eventArgs) => { eventArgs.Cancel = true; shutdown.Cancel(); };

Console.WriteLine($"Starting {options.ClientCount} {options.Serializer} client(s) for {options.DurationSeconds}s...");
var clients = Enumerable.Range(1, options.ClientCount)
    .Select(id => new WeatherPipeClient(id, options, metrics).RunAsync(shutdown.Token)).ToArray();
var reporter = Task.Run(async () =>
{
    using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
    try
    {
        while (await timer.WaitForNextTickAsync(shutdown.Token))
            Console.WriteLine($"[client] {metrics.Format(options.Serializer)}");
    }
    catch (OperationCanceledException) { }
});

await Task.WhenAll(clients);
shutdown.Cancel();
await reporter;
Console.WriteLine($"[final] {metrics.Format(options.Serializer)}");
