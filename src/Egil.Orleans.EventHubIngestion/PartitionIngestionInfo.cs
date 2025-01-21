namespace Egil.Orleans.EventHubIngestion;

internal record class PartitionIngestionInfo(
    string EventHubName,
    string ConsumerGroup,
    string PartitionId,
    string EventGrainType);