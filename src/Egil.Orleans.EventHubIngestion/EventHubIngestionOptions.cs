using Azure.Messaging.EventHubs;

namespace Egil.Orleans.EventHubIngestion;

public record EventHubIngestionOptions<TGrainType> where TGrainType : IGrainWithStringKey
{
    public EventHubOptions EventHubOptions { get; } = new();

    public int MaxBatchSize { get; init; } = 100;

    public TimeSpan MaximumBatchWaitTime { get; set; } = TimeSpan.FromSeconds(10);

    public TimeSpan SaveTrackingStateInterval { get; set; } = TimeSpan.FromSeconds(30);

    public Func<EventData, string> GrainIdSelector { get; set; }
        = _ => throw new InvalidOperationException($"No {typeof(EventHubIngestionOptions<TGrainType>).FullName}.{nameof(GrainIdSelector)} provided.");

    public Func<TGrainType, EventData, CancellationToken, ValueTask> OnEventAsync { get; set; }
        = (x, y, ct) => throw new InvalidOperationException($"No {typeof(EventHubIngestionOptions<TGrainType>).FullName}.{nameof(OnEventAsync)} provided.");
}
