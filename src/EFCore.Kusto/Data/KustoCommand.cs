using System.Collections;
using System.Data;
using System.Data.Common;
using Azure.Core;
using Azure.Identity;
using EFCore.Kusto.Infrastructure.Internal;
using Kusto.Data;
using Kusto.Data.Net.Client;

namespace EFCore.Kusto.Data;

/// <summary>
/// A lightweight <see cref="DbCommand"/> implementation that executes Kusto queries using the Azure SDK.
/// </summary>
public sealed class KustoCommand : DbCommand
{
    private readonly string _clusterUrl;
    private readonly string _database;
    private readonly KustoOptionsExtension _options;
    private readonly TokenCredential _credential;

    /// <summary>
    /// Initializes a new instance of the <see cref="KustoCommand"/> class.
    /// </summary>
    /// <param name="clusterUrl">The target Kusto cluster URL.</param>
    /// <param name="database">The database name within the cluster.</param>
    public KustoCommand(string clusterUrl, string database, KustoOptionsExtension options)
    {
        _clusterUrl = clusterUrl;
        _database = database;
        _options = options;

        if (_options.AuthenticationStrategy == KustoAuthenticationStrategy.Application
            && (string.IsNullOrWhiteSpace(_options.ApplicationTenantId)
                || string.IsNullOrWhiteSpace(_options.ApplicationClientId)
                || string.IsNullOrWhiteSpace(_options.ApplicationClientSecret)))
        {
            throw new InvalidOperationException(
                "Application authentication requires tenant id, client id, and client secret to be configured.");
        }

        _credential = _options.AuthenticationStrategy switch
        {
            KustoAuthenticationStrategy.ManagedIdentity when !string.IsNullOrWhiteSpace(
                    _options.ManagedIdentityClientId)
                => new ManagedIdentityCredential(_options.ManagedIdentityClientId),
            KustoAuthenticationStrategy.ManagedIdentity
                => new ManagedIdentityCredential(),
            KustoAuthenticationStrategy.Application
                => new ClientSecretCredential(
                    _options.ApplicationTenantId!,
                    _options.ApplicationClientId!,
                    _options.ApplicationClientSecret!),
            _ => _options.Credential ?? new DefaultAzureCredential()
        };
    }


    public override string CommandText { get; set; } = string.Empty;
    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; } = CommandType.Text;


    protected override DbConnection? DbConnection { get; set; }

    protected override DbParameterCollection DbParameterCollection => new KustoParameterCollection();

    protected override DbTransaction DbTransaction { get; set; }
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

    public override void Cancel()
    {
    }

    public override int ExecuteNonQuery() => throw new NotSupportedException();
    public override object ExecuteScalar() => throw new NotSupportedException();

    public override void Prepare()
    {
    }

    protected override DbParameter CreateDbParameter() => new KustoParameter();

    /// <summary>
    /// Executes the command against the configured Kusto cluster and returns a <see cref="DbDataReader"/>.
    /// </summary>
    /// <param name="behavior">The command behavior flags.</param>
    /// <returns>A <see cref="DbDataReader"/> that iterates over the query results.</returns>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        => ExecuteDbDataReaderAsync(behavior, CancellationToken.None)
            .ConfigureAwait(false)
            .GetAwaiter()
            .GetResult();

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        var token = await GetKustoTokenAsync(_clusterUrl, cancellationToken).ConfigureAwait(false);

        var csb = new KustoConnectionStringBuilder($"{_clusterUrl};Fed=true")
        {
            UserToken = token,
            InitialCatalog = _database
        };

        var client = KustoClientFactory.CreateCslQueryProvider(csb);
        var admin = KustoClientFactory.CreateCslAdminProvider(csb);

        CommandText = CommandText.Replace("| project  = COUNT(*)", "| count");
        CommandText = CommandText.Replace("\n| project EXISTS ", "");

        IDataReader reader;
        if (CommandText.StartsWith("."))
        {
            reader = admin.ExecuteControlCommand(CommandText);
        }
        else
        {
            reader = client.ExecuteQuery(CommandText);
        }

        return new KustoDataReader(reader, client);
    }

    /// <summary>
    /// Retrieves an access token suitable for authenticating to the configured Kusto cluster.
    /// </summary>
    /// <param name="clusterUrl">The cluster URL to scope the token to.</param>
    /// <returns>An access token string.</returns>
    private async Task<string> GetKustoTokenAsync(string clusterUrl, CancellationToken cancellationToken)
    {
        string scope = $"{clusterUrl}/.default";

        AccessToken token = await _credential.GetTokenAsync(
            new TokenRequestContext(new[] { scope }),
            cancellationToken).ConfigureAwait(false);

        return token.Token;
    }


    private sealed class KustoParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; }
        public override bool IsNullable { get; set; }
        public override string ParameterName { get; set; }
        public override int Size { get; set; }
        public override string SourceColumn { get; set; }
        public override object Value { get; set; }
        public override bool SourceColumnNullMapping { get; set; }

        public override void ResetDbType()
        {
        }
    }

    private sealed class KustoParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _items = new();

        public override int Count => _items.Count;
        public override object SyncRoot => this;

        public override int Add(object value)
        {
            _items.Add((DbParameter)value);
            return _items.Count - 1;
        }

        public override void AddRange(Array values)
        {
            foreach (var v in values) Add(v);
        }

        public override void Clear() => _items.Clear();
        public override bool Contains(object value) => _items.Contains((DbParameter)value);
        public override bool Contains(string value) => _items.Any(p => p.ParameterName == value);
        public override void CopyTo(Array array, int index) => _items.ToArray().CopyTo(array, index);
        public override IEnumerator GetEnumerator() => _items.GetEnumerator();
        public override int IndexOf(object value) => _items.IndexOf((DbParameter)value);
        public override int IndexOf(string parameterName) => _items.FindIndex(p => p.ParameterName == parameterName);
        public override void Insert(int index, object value) => _items.Insert(index, (DbParameter)value);
        public override void Remove(object value) => _items.Remove((DbParameter)value);
        public override void RemoveAt(int index) => _items.RemoveAt(index);
        public override void RemoveAt(string parameterName) => _items.RemoveAt(IndexOf(parameterName));
        protected override DbParameter GetParameter(int index) => _items[index];
        protected override DbParameter GetParameter(string parameterName) => _items[IndexOf(parameterName)];
        protected override void SetParameter(int index, DbParameter value) => _items[index] = value;

        protected override void SetParameter(string parameterName, DbParameter value) =>
            _items[IndexOf(parameterName)] = value;
    }
}