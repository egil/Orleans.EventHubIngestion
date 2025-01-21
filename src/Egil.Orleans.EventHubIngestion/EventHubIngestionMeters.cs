using System.Diagnostics.Metrics;

namespace Egil.Orleans.EventHubIngestion;

public sealed class EventHubIngestionMeters
{
    public static readonly Meter Meter = new("Egil.Orleans.EventHubIngestion", "1.0.0");

    internal static readonly Counter<long> EventsProcessed = Meter.CreateCounter<long>("partition-ingestion-events-processed");
    internal static readonly Counter<long> EventsDeliveryFailed = Meter.CreateCounter<long>("partition-ingestion-events-delivery-failed");
}
