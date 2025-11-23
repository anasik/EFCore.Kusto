using System.Data;
using System.Data.Common;
using EFCore.Kusto.Data;
using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Storage;

public sealed class KustoConnection : RelationalConnection
{
    private readonly string _clusterUrl;
    private readonly string _database;
    private readonly KustoOptionsExtension _options;

    public KustoConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies)
    {
        var opts = dependencies.ContextOptions.FindExtension<KustoOptionsExtension>()
                   ?? throw new InvalidOperationException("Kusto options are not configured. Call UseKusto() when configuring the DbContext.");

        _clusterUrl = opts.ClusterUrl;
        _database = opts.Database;
        _options = opts;
    }

    protected override DbConnection CreateDbConnection()
        => new FakeKustoConnection(_clusterUrl, _database, _options);

    private sealed class FakeKustoConnection(string cluster, string db, KustoOptionsExtension options) : DbConnection
    {
        public override string ConnectionString { get; set; }
        public override string Database => db;
        public override string DataSource => cluster;
        public override string ServerVersion => "Kusto";
        public override ConnectionState State => ConnectionState.Open;

        public override void Open() { }
        public override void Close() { }
        public override void ChangeDatabase(string databaseName) { }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand()
            => new KustoCommand(cluster, db, options);
    }
}