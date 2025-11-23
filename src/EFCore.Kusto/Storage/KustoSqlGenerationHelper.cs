using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Storage;

public sealed class KustoSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
    : RelationalSqlGenerationHelper(dependencies)
{
    // ------------------------------------------------------------
    // KQL doesn’t use SQL terminators
    // ------------------------------------------------------------
    public override string StatementTerminator => string.Empty;
    public override string BatchTerminator => string.Empty;

    // ------------------------------------------------------------
    // KQL doesn’t delimit identifiers
    // ------------------------------------------------------------
    public override string DelimitIdentifier(string name) => name;

    public override string DelimitIdentifier(string name, string? schema) => name;

    // ------------------------------------------------------------
    // KQL doesn’t use @p0 or :p0 style placeholders
    // EF still calls these; we return raw names
    // ------------------------------------------------------------
    public override string GenerateParameterName(string name) => name;

    public override void GenerateParameterName(StringBuilder builder, string name)
        => builder.Append(name);

    public override string GenerateParameterNamePlaceholder(string name) => name;

    public override void GenerateParameterNamePlaceholder(StringBuilder builder, string name)
        => builder.Append(name);
}