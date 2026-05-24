using System.Collections;
using System.Data;
using System.Globalization;
using System.Text.Json;

namespace EFCore.Kusto.Storage;

/// <summary>
/// Single source of truth for converting a CLR value to a KQL literal expression.
/// Used by:
///   - KustoUpdateSqlGenerator — inline literals in raw <c>.update</c> KQL.
///   - KustoCommand — values bound to <c>declare query_parameters</c> placeholders
///     via <c>ClientRequestProperties.SetParameter</c>, which substitutes the
///     returned string as-is into the query text.
///
/// All emissions use the double-quote convention with backslash escaping —
/// consistent with the existing query-side string emission in
/// <c>KustoQuerySqlGenerator.VisitSqlConstant</c>.
/// </summary>
public static class KustoLiteral
{
    /// <summary>
    /// Returns the KQL-literal form of <paramref name="value"/>. When the
    /// value is null, the returned literal is the typed-null appropriate for
    /// <paramref name="kqlType"/> (e.g. <c>guid(null)</c>, <c>datetime(null)</c>).
    /// </summary>
    public static string Format(object? value, string? kqlType)
    {
        if (value is null || value == DBNull.Value)
            return TypedNull(kqlType);

        return value switch
        {
            string s => $"\"{Escape(s)}\"",
            bool b => b ? "true" : "false",
            Guid g => $"guid(\"{g}\")",
            DateTime dt => $"datetime({dt:O})",
            DateTimeOffset o => $"datetime({o.UtcDateTime:O})",
            DateOnly d => $"datetime({d:yyyy-MM-dd})",
            TimeOnly t => $"time({t:HH:mm:ss.fffffff})",
            TimeSpan ts => $"time({ts:c})",
            decimal m => $"decimal({m.ToString(CultureInfo.InvariantCulture)})",
            double dbl => $"real({dbl.ToString(CultureInfo.InvariantCulture)})",
            float f => $"real({f.ToString(CultureInfo.InvariantCulture)})",
            byte or sbyte or short or ushort or int
                => $"int({Convert.ToString(value, CultureInfo.InvariantCulture)})",
            uint or long or ulong
                => Convert.ToString(value, CultureInfo.InvariantCulture)!,
            IEnumerable e => $"\"{Escape(JsonSerializer.Serialize(e))}\"",
            _ => $"dynamic({JsonSerializer.Serialize(value)})",
        };
    }

    private static string TypedNull(string? kqlType) => kqlType switch
    {
        "string" => "\"\"",
        "guid" => "guid(null)",
        "bool" => "bool(null)",
        "date" or "datetime" => "datetime(null)",
        "int" => "int(null)",
        "long" => "long(null)",
        "real" => "real(null)",
        "decimal" => "decimal(null)",
        "double" => "double(null)",
        "timespan" => "timespan(null)",
        "dynamic" => "dynamic(null)",
        _ => "dynamic(null)",
    };

    private static string Escape(string s)
        => s.Replace("\\", "\\\\")
            .Replace("\"", "\\\"")
            .Replace("\r", "\\r")
            .Replace("\n", "\\n");
}