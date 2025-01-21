namespace Egil.Orleans.EventHubIngestion;

internal static class PartitionIngestionGrainExtensions
{
    internal static IPartitionIngestionGrain<TEventGrain> GetPartitionIngestionGrain<TEventGrain>(
        this IGrainFactory factory,
        string eventHubName,
        string consumerGroup,
        string partitionId)
        where TEventGrain : IGrainWithStringKey
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentException.ThrowIfNullOrWhiteSpace(eventHubName);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionId);

        return factory.GetGrain<IPartitionIngestionGrain<TEventGrain>>($"{eventHubName}:{consumerGroup}:{partitionId}");
    }

    internal static PartitionIngestionInfo GetPartitionIngestionInfo<TEventGrain>(
        this IPartitionIngestionGrain<TEventGrain> ingestionGrain)
        where TEventGrain : IGrainWithStringKey
    {
        var parts = ingestionGrain.GetPrimaryKeyString().Split(':');
        var eventHub = parts[0];
        var consumerGroup = parts[1];
        var partitionId = parts[2];
        return new PartitionIngestionInfo(eventHub, consumerGroup, partitionId, typeof(TEventGrain).Name);
    }
}
