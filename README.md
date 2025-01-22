# Fast? ingestion of events from Azure Event Hub

If you have a grain that should receive events from Azure Event Hub, e.g.:

```c#
public interface IProcessEventGrain : IGrainWithStringKey
{
    ValueTask ProcessAsync([Immutable] ReadOnlyMemory<byte> body, long sequenceNumber);
}

[Immutable, GenerateSerializer]
public record class EventProcessingState(
    DateTimeOffset ReceivedTimestamp,
    int EventCount,
    long SequenceNumber);

[PreferLocalPlacement]
internal sealed class ProcessEventGrain(
    [PersistentState("EventProcessingState")] IPersistentState<EventProcessingState> state,
    ILogger<ProcessEventGrain> logger) : Grain, IProcessEventGrain
{
    private static readonly JsonSerializerOptions SerializerOptions = new();

    public async ValueTask ProcessAsync([Immutable] ReadOnlyMemory<byte> body, long sequenceNumber)
    {
        if (state.State.SequenceNumber > 0 && state.State.SequenceNumber >= sequenceNumber)
        {
            logger.LogWarning("Received an older message. Current sequence number = {CurrentSequenceNumber}. Received sequence number = {ReceivedSequenceNumber}.", state.State.SequenceNumber, sequenceNumber);
            return;
        }

        var @event = JsonSerializer.Deserialize<MyEventType>(body.Span, SerializerOptions);
        
        // do things with event
        return ValueTask.CompletedTask;
    }
}
```

You can configure the silo to ingest events from a specific consumer group. The code will spin up one `PartitionIngestionGrain` per partition on the target EventHub.

```c#
hostBuilder.UseOrleans(siloBuilder =>
{
    siloBuilder.Configure<EventHubIngestionOptions<IProcessEventGrain>>(x =>
    {
        // Provide a func that extracts the ID of the grain that should
        // receive the event from the event data.
        x.GrainIdSelector = static (EventData data) => data.PartitionKey;

        // Provide a func that is used to invoke the specific method on the target grain
        // and provide the event data to it.
        x.OnEventAsync = static (IProcessEventGrain grain, EventData data, CancellationToken cancellationToken)
            => grain.ProcessAsync(data.EventBody.ToMemory(), data.SequenceNumber);

        // Use one of the ConfigureEventHubConnection method to set up the event hub connection options.
        x.EventHubOptions.ConfigureEventHubConnection(
            "FQDN FOR EVENT HUB NAMESPACE",
            "EVENT HUB NAME",
            "CONSUMER GROUP",
            new DefaultAzureCredential(new DefaultAzureCredentialOptions()));
    });

}

// Register a background service that will ensure that ingestion is initiated when the silo starts up.
// Has to happen after hostBuilder.UseOrleans(...) to ensure Orleans is ready to run.
hostBuilder.Services.AddHostedService<PartitionIngestionInitializationService<IProcessEventGrain>>();
```
