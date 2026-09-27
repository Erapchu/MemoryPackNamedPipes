namespace Weather.Contracts;

public sealed class LatencySummary
{
    private readonly object _gate = new();
    private readonly double[] _samples;
    private long _totalCount;
    private double _totalMicroseconds;
    private int _next;
    private int _sampleCount;

    public LatencySummary(int capacity = 32_768) => _samples = new double[capacity];

    public void Record(TimeSpan elapsed)
    {
        var microseconds = elapsed.TotalMicroseconds;
        lock (_gate)
        {
            _totalCount++;
            _totalMicroseconds += microseconds;
            _samples[_next] = microseconds;
            _next = (_next + 1) % _samples.Length;
            _sampleCount = Math.Min(_sampleCount + 1, _samples.Length);
        }
    }

    public Snapshot GetSnapshot()
    {
        lock (_gate)
        {
            if (_sampleCount == 0) return new Snapshot(_totalCount, 0, 0, 0, 0);
            var copy = new double[_sampleCount];
            if (_sampleCount < _samples.Length) Array.Copy(_samples, copy, _sampleCount);
            else
            {
                var tail = _samples.Length - _next;
                Array.Copy(_samples, _next, copy, 0, tail);
                Array.Copy(_samples, 0, copy, tail, _next);
            }
            Array.Sort(copy);
            return new Snapshot(_totalCount, _totalMicroseconds / _totalCount,
                Percentile(copy, .50), Percentile(copy, .95), Percentile(copy, .99));
        }
    }

    private static double Percentile(double[] sorted, double percentile) =>
        sorted[(int)Math.Clamp(Math.Ceiling(percentile * sorted.Length) - 1, 0, sorted.Length - 1)];

    public readonly record struct Snapshot(long Count, double MeanUs, double P50Us, double P95Us, double P99Us);
}
