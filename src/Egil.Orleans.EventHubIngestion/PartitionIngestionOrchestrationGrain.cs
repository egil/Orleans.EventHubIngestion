using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Egil.Orleans.EventHubIngestion;

internal interface IPartitionIngestionOrchestrationGrain<TEventGrain> : IGrainWithStringKey
    where TEventGrain : IGrainWithStringKey
{
    Task InitializeAsync();
}

internal sealed partial class PartitionIngestionOrchestrationGrain<TEventGrain>(
    IOptions<EventHubIngestionOptions<TEventGrain>> options,
    ILogger<PartitionIngestionOrchestrationGrain<TEventGrain>> logger) : Grain, IPartitionIngestionOrchestrationGrain<TEventGrain>, IRemindable
    where TEventGrain : IGrainWithStringKey
{
    private string[] partitionIds = [];

    public async Task InitializeAsync()
    {
        await using var connection = options.Value.EventHubOptions.CreateConnection();
        await using var eventHubClient = new EventHubConsumerClient(
            options.Value.EventHubOptions.ConsumerGroup,
            connection);

        partitionIds = await eventHubClient.GetPartitionIdsAsync();

        LogEnsuringPartitionIngestionGrainIsRunning(
            partitionIds.Length,
            typeof(TEventGrain).Name,
            options.Value.EventHubOptions.EventHubName,
            options.Value.EventHubOptions.ConsumerGroup);

        foreach (var partitionId in partitionIds)
        {
            var grain = GrainFactory.GetPartitionIngestionGrain<TEventGrain>(
                options.Value.EventHubOptions.EventHubName,
                options.Value.EventHubOptions.ConsumerGroup,
                partitionId);


            await grain.EnsureRunningAsync();
        }

        await this.RegisterOrUpdateReminder(
            reminderName: "EnsureIngestionGrainsRunning",
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (partitionIds.Length == 0)
        {
            await InitializeAsync();
        }

        foreach (var partitionId in partitionIds)
        {
            var grain = GrainFactory.GetPartitionIngestionGrain<TEventGrain>(
                options.Value.EventHubOptions.EventHubName,
                options.Value.EventHubOptions.ConsumerGroup,
                partitionId);
            await grain.EnsureRunningAsync();
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Ensuring {PartitionCount} IPartitionIngestionGrain<{EventGrain}> is running for {EventHub} and {ConsumerGroup}")]
    private partial void LogEnsuringPartitionIngestionGrainIsRunning(
        int partitionCount,
        string eventGrain,
        string eventHub,
        string consumerGroup);
}