using System.Data.Common;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Storage;

public class KustoConnection : RelationalConnection
{
    public KustoConnection(RelationalConnectionDependencies dependencies) : base(dependencies)
    {
    }

    protected override DbConnection CreateDbConnection()
    {
        throw new NotImplementedException();
    }
}