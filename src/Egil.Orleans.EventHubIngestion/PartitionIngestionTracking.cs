namespace Egil.Orleans.EventHubIngestion;

[GenerateSerializer, Immutable]
[Alias("Egil.Orleans.EventHubIngestion.PartitionIngestionTracking")]
internal record class PartitionIngestionTracking(
    string PartitionId,
    long LastReadOffset,
    long ProcessedCount,
    DateTimeOffset Updated);
