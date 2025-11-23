using Microsoft.EntityFrameworkCore.Update;

namespace EFCore.Kusto.Update;

public class KustoModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    public ModificationCommandBatch Create()
    {
        throw new NotImplementedException();
    }
}