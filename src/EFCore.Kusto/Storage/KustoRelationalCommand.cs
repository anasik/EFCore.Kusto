using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Storage;

#if NET10_0_OR_GREATER
// EF Core 10 added a separate logCommandText to RelationalCommand so logs can redact sensitive
// literals independently of the executed text. This provider never marks parameters as sensitive
// (Append is called without the sensitive flag), so the log text always equals the command text.
public class KustoRelationalCommand(
    RelationalCommandBuilderDependencies dependencies,
    string commandText,
    string logCommandText,
    IReadOnlyList<IRelationalParameter> parameters)
    : RelationalCommand(dependencies, commandText, logCommandText, parameters)
#else
public class KustoRelationalCommand(
    RelationalCommandBuilderDependencies dependencies,
    string commandText,
    IReadOnlyList<IRelationalParameter> parameters)
    : RelationalCommand(dependencies, commandText, parameters)
#endif
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
#if NET10_0_OR_GREATER
        return new KustoRelationalCommand(Dependencies, ToString(), ToString(), Parameters);
#else
        return new KustoRelationalCommand(Dependencies, ToString(), Parameters);
#endif
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
