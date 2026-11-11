using System.Data.Common;
using EFCore.Kusto.Data;
using Kusto.Language;
using Kusto.Language.Symbols;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.Kusto.ConformanceTests.Infrastructure;

/// <summary>
/// Checks generated KQL offline with Microsoft's Kusto.Language parser and semantic analyzer, against a schema
/// built from the EF model: syntax errors, unknown columns or tables, wrong argument types.
/// </summary>
public sealed class KqlValidator
{
    private readonly GlobalState _globals;

    public KqlValidator(DbContext context)
    {
        var tables = context.Model.GetEntityTypes()
            .Where(e => e.GetTableName() != null)
            .Select(e =>
            {
                var table = StoreObjectIdentifier.Table(e.GetTableName()!, e.GetSchema());
                var columns = e.GetProperties()
                    .Select(p => $"['{p.GetColumnName(table)}']:{p.GetRelationalTypeMapping().StoreType}");
                return new TableSymbol(e.GetTableName()!, "(" + string.Join(", ", columns) + ")");
            })
            .ToArray();
        _globals = GlobalState.Default.WithDatabase(new DatabaseSymbol("db", tables));
    }

    /// <summary>The query text exactly as <see cref="KustoCommand"/> sends it: parameter declarations first.</summary>
    public static string CommandText(IQueryable query)
    {
        using DbCommand command = query.CreateDbCommand();
        // Mirrors KustoCommand.ExecuteDbDataReaderAsync.
        var text = command.CommandText.Replace("\n| project EXISTS ", "");
        if (command.Parameters.Count == 0)
            return text;

        var declarations = command.Parameters.Cast<DbParameter>()
            .Select(p => $"{p.ParameterName}:{KustoCommand.GetKustoType(p.DbType)}");
        return $"declare query_parameters ({string.Join(", ", declarations)});\n{text}";
    }

    /// <summary>Returns the analyzer's errors for <paramref name="kql"/>, one per line, or an empty list.</summary>
    public IReadOnlyList<string> Errors(string kql)
    {
        var code = KustoCode.ParseAndAnalyze(kql, _globals);
        return code.GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .Select(d => $"{d.Message} at '{Snippet(kql, d.Start, d.Length)}'")
            .ToList();
    }

    private static string Snippet(string text, int start, int length)
    {
        start = Math.Clamp(start, 0, text.Length);
        return text.Substring(start, Math.Clamp(Math.Max(length, 1) + 20, 0, text.Length - start)).ReplaceLineEndings(" ");
    }
}
