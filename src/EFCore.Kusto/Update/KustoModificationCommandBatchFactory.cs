using Microsoft.EntityFrameworkCore.Update;

namespace EFCore.Kusto.Update;

public class KustoModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    : IModificationCommandBatchFactory
{
    public ModificationCommandBatch Create()
    {
        return new SingularModificationCommandBatch(dependencies);
    }
}