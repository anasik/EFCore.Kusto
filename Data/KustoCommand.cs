using System.Collections;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using Azure.Core;
using Azure.Identity;
using EFCore.Kusto.Extensions;
using EFCore.Kusto.Storage;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;

namespace EFCore.Kusto.Data;

public sealed class KustoCommand : DbCommand
{
    private readonly string _clusterUrl;
    private readonly string _database;

    public KustoCommand(string clusterUrl, string database)
    {
        _clusterUrl = clusterUrl;
        _database = database;
    }


    public override string CommandText { get; set; } = "";
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

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var token = GetKustoTokenAsync(_clusterUrl).Result;

        var csb = new KustoConnectionStringBuilder($"{_clusterUrl};Fed=true")
        {
            UserToken = token,
            InitialCatalog = _database
        };

        var client = KustoClientFactory.CreateCslQueryProvider(csb);

        var reader = client.ExecuteQuery(CommandText);

        return new KustoDataReader(reader, client);
    }

    private async Task<string> GetKustoTokenAsync(string clusterUrl)
    {
        var credential = new DefaultAzureCredential();

        // Kusto requires scope: "https://<cluster>.kusto.windows.net/.default"
        string scope = $"{clusterUrl}/.default";

        AccessToken token = await credential.GetTokenAsync(
            new TokenRequestContext(new[] { scope }));

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