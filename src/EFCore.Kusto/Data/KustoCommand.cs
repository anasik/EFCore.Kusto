using System.Collections;
using System.Data;
using System.Data.Common;
using System.Linq;
using Azure.Core;
using Azure.Identity;
using EFCore.Kusto.Infrastructure.Internal;
using Kusto.Data;
using Kusto.Data.Common;
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

    private readonly KustoParameterCollection _parameters = new();

    protected override DbParameterCollection DbParameterCollection => _parameters;

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
        var crp = new ClientRequestProperties();

        CommandText = PrepareCommandText(CommandText, Parameters, crp);
        var isControlCommand = CommandText.TrimStart().StartsWith(".");

        Func<string, IDataReader> Execute = isControlCommand
            ? text =>
                admin.ExecuteControlCommand(text, crp)
            : text => client.ExecuteQuery(text, crp);

        return new KustoDataReader(Execute(CommandText), client);
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

    private static string GetKustoType(DbType dbType) => dbType switch
    {
        DbType.AnsiString or DbType.String or DbType.StringFixedLength or DbType.AnsiStringFixedLength
            => "string",
        DbType.Int16 or DbType.Int32 or DbType.Int64 => "long",
        DbType.Boolean => "bool",
        DbType.DateTime or DbType.Date or DbType.Time => "datetime",
        DbType.Double or DbType.Decimal or DbType.Single => "real",
        DbType.Guid => "guid",
        _ => "string"
    };

    internal static string PrepareCommandText(
        string commandText,
        DbParameterCollection parameters,
        ClientRequestProperties requestProperties)
    {
        var normalizedText = commandText
            .Replace("| project COUNT(*)", "| count")
            .Replace("\n| project EXISTS ", "");

        if (parameters.Count == 0 || normalizedText.TrimStart().StartsWith("."))
            return normalizedText;

        var uniqueParameters = new List<(string Name, DbType DbType, object? Value)>();
        var parameterIndexByName = new Dictionary<string, int>(StringComparer.Ordinal);

        for (var i = 0; i < parameters.Count; i++)
        {
            if (parameters[i] is not DbParameter parameter)
                continue;

            var normalizedName = NormalizeParameterName(parameter.ParameterName);
            var parameterValue = NormalizeParameterValue(parameter.DbType, parameter.Value);

            if (parameterIndexByName.TryGetValue(normalizedName, out var existingIndex))
            {
                var existing = uniqueParameters[existingIndex];
                if (existing.DbType != parameter.DbType ||
                    !Equals(existing.Value, parameterValue))
                {
                    throw new InvalidOperationException(
                        $"Duplicate Kusto parameter '{normalizedName}' was generated with conflicting definitions.");
                }

                continue;
            }

            parameterIndexByName[normalizedName] = uniqueParameters.Count;
            uniqueParameters.Add((normalizedName, parameter.DbType, parameterValue));
        }

        for (var i = 0; i < uniqueParameters.Count; i++)
        {
            ApplyParameter(
                requestProperties,
                uniqueParameters[i].Name,
                uniqueParameters[i].DbType,
                uniqueParameters[i].Value);
        }

        var header = "declare query_parameters ("
                     + string.Join(
                         ", ",
                         uniqueParameters.Select(parameter => $"{parameter.Name}:{GetKustoType(parameter.DbType)}"))
                     + ");\n";

        return header + normalizedText;
    }

    private static string NormalizeParameterName(string parameterName)
    {
        if (parameterName.StartsWith("__", StringComparison.Ordinal))
            return parameterName[2..];

        if (parameterName.StartsWith("@__", StringComparison.Ordinal))
            return parameterName[3..];

        if (parameterName.StartsWith("@", StringComparison.Ordinal))
            return parameterName[1..];

        return parameterName;
    }

    internal static object? NormalizeParameterValue(DbType dbType, object? value)
    {
        if (value is null or DBNull)
        {
            return null;
        }

        return dbType switch
        {
            DbType.Int16 or DbType.Int32 or DbType.Int64 => Convert.ToInt64(value),
            DbType.Boolean => Convert.ToBoolean(value),
            DbType.Double or DbType.Decimal or DbType.Single => Convert.ToDouble(value),
            DbType.Guid => value is Guid guid ? guid : Guid.Parse(value.ToString()!),
            DbType.DateTime or DbType.Date => value is DateTime dateTime
                ? dateTime
                : Convert.ToDateTime(value),
            DbType.Time => value is TimeSpan timeSpan
                ? timeSpan
                : TimeSpan.Parse(value.ToString()!),
            _ => value.ToString()
        };
    }

    internal static void ApplyParameter(
        ClientRequestProperties requestProperties,
        string parameterName,
        DbType dbType,
        object? value)
    {
        switch (dbType)
        {
            case DbType.Int16:
            case DbType.Int32:
            case DbType.Int64:
                requestProperties.SetParameter(parameterName, value is null ? null : Convert.ToInt64(value));
                break;
            case DbType.Boolean:
                requestProperties.SetParameter(parameterName, value as bool? ?? (value is null ? null : Convert.ToBoolean(value)));
                break;
            case DbType.Double:
            case DbType.Decimal:
            case DbType.Single:
                requestProperties.SetParameter(parameterName, value is null ? null : Convert.ToDouble(value));
                break;
            case DbType.Guid:
                requestProperties.SetParameter(parameterName, value as Guid? ?? (value is null ? null : Guid.Parse(value.ToString()!)));
                break;
            case DbType.Date:
            case DbType.DateTime:
                requestProperties.SetParameter(
                    parameterName,
                    value as DateTime? ?? (value is null ? null : Convert.ToDateTime(value)));
                break;
            case DbType.Time:
                requestProperties.SetParameter(
                    parameterName,
                    value as TimeSpan? ?? (value is null ? null : TimeSpan.Parse(value.ToString()!)));
                break;
            default:
                requestProperties.SetParameter(parameterName, value?.ToString());
                break;
        }
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
