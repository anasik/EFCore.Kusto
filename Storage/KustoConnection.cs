using System.Data;
using System.Data.Common;
using EFCore.Kusto.Data;
using EFCore.Kusto.Infrastructure.Internal;
using Kusto.Data;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Storage;

public sealed class KustoConnection : RelationalConnection
{
    private readonly string _clusterUrl;
    private readonly string _database;

    public KustoConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies)
    {
        var opts = dependencies.ContextOptions.FindExtension<KustoOptionsExtension>();

        _clusterUrl = opts.ClusterUrl;
        _database = opts.Database;
    }

    protected override DbConnection CreateDbConnection()
        => new FakeKustoConnection(_clusterUrl, _database);

    private sealed class FakeKustoConnection : DbConnection
    {
        private readonly string _cluster;
        private readonly string _db;

        public FakeKustoConnection(string cluster, string db)
        {
            _cluster = cluster;
            _db = db;
        }

        public override string ConnectionString { get; set; }
        public override string Database => _db;
        public override string DataSource => _cluster;
        public override string ServerVersion => "Kusto";
        public override ConnectionState State => ConnectionState.Open;

        public override void Open() { }
        public override void Close() { }
        public override void ChangeDatabase(string databaseName) { }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand()
            => new KustoCommand(_cluster, _db);
    }
}