using Azure;
using Azure.Core;
using Azure.Messaging.EventHubs;

namespace Egil.Orleans.EventHubIngestion;

/// <summary>
/// EventHub settings for a specific hub
/// </summary>
public sealed class EventHubOptions
{
    private Func<EventHubConnectionOptions, EventHubConnection> createConnection = static _ => throw new InvalidOperationException("Azure Event Hub connection not configured.");

    /// <summary>
    /// Event Hub consumer group.
    /// </summary>
    internal string ConsumerGroup { get; private set; } = string.Empty;

    /// <summary>
    /// Event Hub name.
    /// </summary>
    internal string EventHubName { get; private set; } = string.Empty;

    /// <summary>
    /// Optional parameter that configures the receiver prefetch count.
    /// </summary>
    public int PrefetchCount { get; set; } = 100;

    /// <summary>
    /// Connection options used when creating a connection to an Azure Event Hub.
    /// </summary>
    public EventHubConnectionOptions ConnectionOptions { get; set; } = new EventHubConnectionOptions { TransportType = EventHubsTransportType.AmqpTcp };

    /// <summary>
    /// Configures the Azure Event Hub connection using the provided connection string.
    /// </summary>
    public void ConfigureEventHubConnection(string connectionString, string eventHubName, string consumerGroup)
    {
        EventHubName = eventHubName;
        ConsumerGroup = consumerGroup;

        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        ValidateValues(eventHubName, consumerGroup);

        createConnection = connectionOptions => new EventHubConnection(connectionString, EventHubName, connectionOptions);
    }

    /// <summary>
    /// Configures the Azure Event Hub connection using the provided fully-qualified namespace string and credential.
    /// </summary>
    public void ConfigureEventHubConnection(string fullyQualifiedNamespace, string eventHubName, string consumerGroup, AzureNamedKeyCredential credential)
    {
        EventHubName = eventHubName;
        ConsumerGroup = consumerGroup;

        ArgumentException.ThrowIfNullOrWhiteSpace(fullyQualifiedNamespace);

        ValidateValues(eventHubName, consumerGroup);

        ArgumentNullException.ThrowIfNull(credential);

        createConnection = connectionOptions => new EventHubConnection(fullyQualifiedNamespace, EventHubName, credential, connectionOptions);
    }

    /// <summary>
    /// Configures the Azure Event Hub connection using the provided fully-qualified namespace string and credential.
    /// </summary>
    public void ConfigureEventHubConnection(string fullyQualifiedNamespace, string eventHubName, string consumerGroup, AzureSasCredential credential)
    {
        EventHubName = eventHubName;
        ConsumerGroup = consumerGroup;

        ArgumentException.ThrowIfNullOrWhiteSpace(fullyQualifiedNamespace);

        ValidateValues(eventHubName, consumerGroup);

        ArgumentNullException.ThrowIfNull(credential);

        createConnection = connectionOptions => new EventHubConnection(fullyQualifiedNamespace, EventHubName, credential, connectionOptions);
    }

    /// <summary>
    /// Configures the Azure Event Hub connection using the provided fully-qualified namespace string and credential.
    /// </summary>
    public void ConfigureEventHubConnection(string fullyQualifiedNamespace, string eventHubName, string consumerGroup, TokenCredential credential)
    {
        EventHubName = eventHubName;
        ConsumerGroup = consumerGroup;

        ArgumentException.ThrowIfNullOrWhiteSpace(fullyQualifiedNamespace);

        ValidateValues(eventHubName, consumerGroup);

        ArgumentNullException.ThrowIfNull(credential);

        createConnection = connectionOptions => new EventHubConnection(fullyQualifiedNamespace, EventHubName, credential, connectionOptions);
    }

    /// <summary>
    /// Configures the Azure Event Hub connection using the provided connection instance.
    /// </summary>
    public void ConfigureEventHubConnection(EventHubConnection connection, string consumerGroup)
    {
        ArgumentNullException.ThrowIfNull(connection);
        EventHubName = connection.EventHubName;
        ConsumerGroup = consumerGroup;
        ValidateValues(connection.EventHubName, consumerGroup);
        createConnection = _ => connection;
    }

    internal EventHubConnection CreateConnection() => createConnection.Invoke(ConnectionOptions);

    private void ValidateValues(string eventHubName, string consumerGroup)
    {
        if (string.IsNullOrWhiteSpace(eventHubName))
        {
            throw new ArgumentException("A non-null, non-empty value must be provided.", nameof(eventHubName));
        }

        if (string.IsNullOrWhiteSpace(consumerGroup))
        {
            throw new ArgumentException("A non-null, non-empty value must be provided.", nameof(consumerGroup));
        }
    }
}
