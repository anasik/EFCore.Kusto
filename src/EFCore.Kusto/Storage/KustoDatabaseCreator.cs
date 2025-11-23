using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Storage;

internal class KustoDatabaseCreator : RelationalDatabaseCreator
{
    public KustoDatabaseCreator(RelationalDatabaseCreatorDependencies dependencies) : base(dependencies)
    {
    }

    public override bool Exists()
    {
        return true;
    }

    public override bool HasTables()
    {
        return true;
    }

    public override void Create()
    {
        return;
    }

    public override void Delete()
    {
        return;
    }
}