using Microsoft.Extensions.Hosting;

namespace Egil.Orleans.EventHubIngestion;

public sealed class PartitionIngestionInitializationService<TEventGrain>(IGrainFactory grainFactory) : BackgroundService
    where TEventGrain : IGrainWithStringKey
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var name = typeof(TEventGrain).FullName ?? typeof(TEventGrain).Name;
        var gig = grainFactory.GetGrain<IPartitionIngestionOrchestrationGrain<TEventGrain>>(name);
        await gig.InitializeAsync();
    }
}
