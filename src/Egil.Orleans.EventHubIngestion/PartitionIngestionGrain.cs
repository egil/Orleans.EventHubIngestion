using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Primitives;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Placement;

namespace Egil.Orleans.EventHubIngestion;

public interface IPartitionIngestionGrain<TEventGrain> : IGrainWithStringKey
    where TEventGrain : IGrainWithStringKey
{
    Task EnsureRunningAsync();
}

/// <summary>
/// This grain is responsible for ingesting events from a particular
/// EventHub partition and sending them to the appropriate grain.
/// It is Reentrant so that the ReceiveReminder method is able to be invoked
/// while the grain is processing events concurrently,
/// and since the processing of events can be resource intensive and there can be
/// multiple instances, one for each partition in the event hub, ResourceOptimizedPlacement
/// attribute will hopefully place instances of this grain on different silos to
/// balance load.
/// </summary>
[ResourceOptimizedPlacement]
internal sealed partial class PartitionIngestionGrain<TEventGrain>
    : Grain, IPartitionIngestionGrain<TEventGrain>, IDisposable
    where TEventGrain : IGrainWithStringKey
{
    private readonly PartitionIngestionInfo info;
    private readonly TagList eventsProcessedTagList;
    private readonly EventHubIngestionOptions<TEventGrain> options;
    private readonly IPersistentState<PartitionIngestionTracking> ingestionTracking;
    private readonly TimeProvider timeProvider;
    private readonly ILogger<PartitionIngestionGrain<TEventGrain>> logger;
    private CancellationTokenSource? ingestionCts;
    private Task? ingestionTask;

    public PartitionIngestionGrain(
        [PersistentState("PartitionIngestionTracking")] IPersistentState<PartitionIngestionTracking> ingestionTracking,
        TimeProvider timeProvider,
        IOptions<EventHubIngestionOptions<TEventGrain>> options,
        ILogger<PartitionIngestionGrain<TEventGrain>> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        this.options = options.Value;
        this.ingestionTracking = ingestionTracking;
        this.timeProvider = timeProvider;
        this.logger = logger;

        info = this.GetPartitionIngestionInfo();

        eventsProcessedTagList = new TagList(
        [
            new("EventHubName", info.EventHubName),
            new("ConsumerGroup", info.ConsumerGroup),
            new("PartitionId", info.PartitionId),
            new("EventGrainType", info.EventGrainType),
        ]);

        Debug.Assert(this.options.EventHubOptions.EventHubName == info.EventHubName);
        Debug.Assert(this.options.EventHubOptions.ConsumerGroup == info.ConsumerGroup);
    }

    public void Dispose()
    {
        ingestionCts?.Dispose();
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
        => StopAsync();

    public async Task EnsureRunningAsync()
    {
        if (ingestionTask is null || ingestionTask.Status is TaskStatus.Faulted or TaskStatus.RanToCompletion)
        {
            ingestionCts = EnsureCts();
            LogStartingPartitionIngestion(info);

            ingestionTracking.State = ingestionTracking.State with
            {
                PartitionId = info.PartitionId,
                Updated = timeProvider.GetUtcNow(),
            };

            await ingestionTracking.WriteStateAsync();

            ingestionTask = Task.Factory.StartNew(
                () => InitializeIngestion(ingestionCts.Token),
                cancellationToken: ingestionCts.Token,
                creationOptions: TaskCreationOptions.LongRunning | TaskCreationOptions.RunContinuationsAsynchronously,
                scheduler: TaskScheduler.Current).Unwrap();
        }

        DelayDeactivation(TimeSpan.FromMinutes(2));
    }

    private async Task StopAsync()
    {
        if (ingestionTask is { } task && ingestionTask.Status is not TaskStatus.Faulted or TaskStatus.RanToCompletion)
        {
            LogStoppingPartitionIngestion(info);
            await (ingestionCts?.CancelAsync() ?? Task.CompletedTask);
            await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);
            await ingestionTracking.WriteStateAsync();
        }
    }

    private async Task InitializeIngestion(CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = options.EventHubOptions.CreateConnection();

            var readOptions = new PartitionReceiverOptions
            {
                PrefetchCount = options.EventHubOptions.PrefetchCount,
                Identifier = this.GetPrimaryKeyString(),
                DefaultMaximumReceiveWaitTime = options.MaximumBatchWaitTime,
            };

            await using var receiver = new PartitionReceiver(
                options.EventHubOptions.ConsumerGroup,
                partitionId: info.PartitionId,
                EventPosition.FromOffset(ingestionTracking.State.LastReadOffset, isInclusive: false),
                connection,
                readOptions);

            await RunIngestion(receiver, cancellationToken);
        }
        catch (Exception ex)
        {
            LogErrorPartitionIngestion(ex, info, ingestionTracking.State);

            // Try to save current state before rethrowing, if that fails, a restart will continue
            // from the last save.
            await ingestionTracking.WriteStateAsync().ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);

            throw;
        }
    }

    private async Task RunIngestion(PartitionReceiver receiver, CancellationToken cancellationToken)
    {
        // start by reading the first batch and then in while loop
        // read the next batch while the previous one is being processed
        var batch = await receiver.ReceiveBatchAsync(options.MaxBatchSize, cancellationToken);
        while (!cancellationToken.IsCancellationRequested)
        {
            var processTask = ProcessBatch(batch, cancellationToken);
            var batchTask = receiver.ReceiveBatchAsync(options.MaxBatchSize, cancellationToken);
            await Task.WhenAll(processTask, batchTask);
            batch = await batchTask;
        }
    }

    private async Task ProcessBatch(IEnumerable<EventData> events, CancellationToken cancellationToken)
    {
        // this fans out to process each grain's batch of events in parallel,
        // such that each grain processes its own events in order.
        var perGrainProcessTasks = events
            .GroupBy(x => options.GrainIdSelector(x))
            .Select(group => ProcessGrainBatch(group.Key, group, cancellationToken));

        var processResults = await Task.WhenAll(perGrainProcessTasks);

        // When cancellation is requested during processing it may indicate that not
        // all events were successfully sent to the target grains, so skip updating
        // tracking state and throw. That way all events in this batch will be resent.
        cancellationToken.ThrowIfCancellationRequested();

        await UpdateTrackingState(processResults);
    }

    private async Task UpdateTrackingState((int Processed, long LastOffset)[] processResults)
    {
        if (processResults.Length == 0)
        {
            return;
        }

        var offset = processResults.Max(x => x.LastOffset);
        var total = processResults.Sum(x => x.Processed);

        // only update the offset if we processed at least one event.
        // offset will be 0 if no events were processed by a ProcessGrainBatch task.
        ingestionTracking.State = ingestionTracking.State with
        {
            LastReadOffset = offset > 0 ? offset : ingestionTracking.State.LastReadOffset,
            ProcessedCount = ingestionTracking.State.ProcessedCount + total,
        };

        if (ingestionTracking.State.Updated + options.SaveTrackingStateInterval < timeProvider.GetUtcNow())
        {
            ingestionTracking.State = ingestionTracking.State with { Updated = timeProvider.GetUtcNow() };
            LogSavingPartitionIngestionState(info, ingestionTracking.State);
            await ingestionTracking.WriteStateAsync();
        }

        EventHubIngestionMeters.EventsProcessed.Add(total, eventsProcessedTagList);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Don't care about the specific exception.")]
    private async Task<(int Processed, long LastOffset)> ProcessGrainBatch(
        string targetGrainId,
        IEnumerable<EventData> events,
        CancellationToken cancellationToken)
    {
        var grain = GrainFactory.GetGrain<TEventGrain>(targetGrainId);
        int processed = 0;
        long lastOffset = 0;

        foreach (var eventData in events)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                LogSendingEventDataToGrainCancelled(info, processed);
                return (processed, lastOffset);
            }

            try
            {
                await options.OnEventAsync(grain, eventData, cancellationToken);
            }
            catch (Exception ex)
            {
                EventHubIngestionMeters.EventsDeliveryFailed.Add(1, eventsProcessedTagList);
                LogErrorSendingEventDataToGrain(ex, info, eventData.SequenceNumber);
            }

            lastOffset = eventData.Offset;
            processed++;
        }

        return (processed, lastOffset);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Cts cannot be disposed twice. Catch all is fine here.")]
    private CancellationTokenSource EnsureCts()
    {
        if (ingestionCts is not null && ingestionCts.TryReset())
        {
            return ingestionCts;
        }

        try
        {
            ingestionCts?.Cancel();
            ingestionCts?.Dispose();
        }
        catch
        {
            // ignore any exceptions from disposing the previous token source
        }

        ingestionCts = new();
        return ingestionCts;
    }

    [LoggerMessage(LogLevel.Information, "Starting partition ingestion. {Info}")]
    private partial void LogStartingPartitionIngestion(PartitionIngestionInfo info);

    [LoggerMessage(LogLevel.Information, "Stopping partition ingestion. {Info}")]
    private partial void LogStoppingPartitionIngestion(PartitionIngestionInfo info);

    [LoggerMessage(LogLevel.Information, "Saving partition ingestion state. {Info}. {Stats}")]
    private partial void LogSavingPartitionIngestionState(PartitionIngestionInfo info, PartitionIngestionTracking stats);

    [LoggerMessage(LogLevel.Error, "Error ingestion events. {Info}. {Stats}")]
    private partial void LogErrorPartitionIngestion(Exception exception, PartitionIngestionInfo info, PartitionIngestionTracking stats);

    [LoggerMessage(LogLevel.Warning, "Sending events cancelled. {Info}. Completed {ProcessedSuccessfully}.")]
    private partial void LogSendingEventDataToGrainCancelled(PartitionIngestionInfo info, int processedSuccessfully);

    [LoggerMessage(LogLevel.Error, "Error sending events. {Info}. Failed event has sequence number: {SequenceNumber}.")]
    private partial void LogErrorSendingEventDataToGrain(Exception exception, PartitionIngestionInfo info, long sequenceNumber);
}
