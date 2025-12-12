using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Storage;

public class KustoRelationalCommand(
    RelationalCommandBuilderDependencies dependencies,
    string commandText,
    IReadOnlyList<IRelationalParameter> parameters)
    : RelationalCommand(dependencies, commandText, parameters)
{
    public override DbCommand CreateDbCommand(
        RelationalCommandParameterObject parameterObject,
        Guid commandId,
        DbCommandMethod commandMethod)
    {
        DbCommand command = base.CreateDbCommand(parameterObject, commandId, commandMethod);

        foreach (DbParameter commandParameter in command.Parameters)
        {
            string name = commandParameter.ParameterName;

            if (name.StartsWith("__"))
            {
                commandParameter.ParameterName = name.Substring(2);
            }
        }

        return command;
    }
}

public class KustoRelationalCommandBuilder(RelationalCommandBuilderDependencies dependencies)
    : RelationalCommandBuilder(dependencies)
{
    public override IRelationalCommand Build()
    {
        return new KustoRelationalCommand(Dependencies, ToString(), Parameters);
    }
}

public class KustoRelationalCommandBuilderFactory(RelationalCommandBuilderDependencies dependencies)
    : RelationalCommandBuilderFactory(dependencies)
{
    public override IRelationalCommandBuilder Create()
    {
        return new KustoRelationalCommandBuilder(Dependencies);
    }
}