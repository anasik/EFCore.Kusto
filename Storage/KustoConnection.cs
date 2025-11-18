using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore.Storage;
using EFCore.Kusto.Infrastructure.Internal;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;

namespace EFCore.Kusto.Storage;

public sealed class KustoConnection : RelationalConnection
{
    private ICslQueryProvider _client;

    public KustoConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies)
    {
        var opts = dependencies.ContextOptions.FindExtension<KustoOptionsExtension>()
                   ?? throw new InvalidOperationException("KustoOptionsExtension missing.");

        var csb = new KustoConnectionStringBuilder(opts.ClusterUrl)
            .WithAadUserPromptAuthentication();

        _client = KustoClientFactory.CreateCslQueryProvider(csb);
    }

    public ICslQueryProvider Client => _client;

    // ---------------------------------------------------------
    // RelationalConnection requires these overrides
    // ---------------------------------------------------------

    public override DbConnection DbConnection => _fake;

    private readonly DbConnection _fake = new FakeDbConnection();

    protected override DbConnection CreateDbConnection()
        => _fake;

    private sealed class FakeDbConnection : DbConnection
    {
        public override string ConnectionString { get; set; } = "KUSTO_FAKE";
        public override string Database => "Kusto";
        public override string DataSource => "Kusto";
        public override string ServerVersion => "Kusto";
        public override ConnectionState State => ConnectionState.Open;

        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand()
            => throw new NotSupportedException();
    }
}
