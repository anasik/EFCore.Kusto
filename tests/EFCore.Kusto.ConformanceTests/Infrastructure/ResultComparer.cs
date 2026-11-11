using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace EFCore.Kusto.ConformanceTests.Infrastructure;

/// <summary>
/// Turns query results into comparable text: one JSON line per row, with numbers, dates and decimals written
/// the same way whether they came from Kusto or from memory.
/// </summary>
public static class ResultComparer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        ReferenceHandler = ReferenceHandler.IgnoreCycles,
        Converters = { new DoubleConverter(), new DecimalConverter(), new DateTimeConverter() },
    };

    public static List<string> Canonical(IEnumerable<object?> rows, bool ordered)
    {
        var lines = rows.Select(r => JsonSerializer.Serialize(r, Options)).ToList();
        if (!ordered)
            lines.Sort(StringComparer.Ordinal);
        return lines;
    }

    public static string Canonical(object? scalar) => JsonSerializer.Serialize(scalar, Options);

    private sealed class DoubleConverter : JsonConverter<double>
    {
        public override double Read(ref Utf8JsonReader reader, Type t, JsonSerializerOptions o) => reader.GetDouble();
        public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions o)
            => writer.WriteRawValue(Math.Round(value, 9).ToString("R", CultureInfo.InvariantCulture));
    }

    private sealed class DecimalConverter : JsonConverter<decimal>
    {
        public override decimal Read(ref Utf8JsonReader reader, Type t, JsonSerializerOptions o) => reader.GetDecimal();
        // 10.50m and 10.5m are equal; Kusto doesn't keep trailing zeros.
        public override void Write(Utf8JsonWriter writer, decimal value, JsonSerializerOptions o)
            => writer.WriteRawValue(value.ToString("G29", CultureInfo.InvariantCulture));
    }

    private sealed class DateTimeConverter : JsonConverter<DateTime>
    {
        public override DateTime Read(ref Utf8JsonReader reader, Type t, JsonSerializerOptions o) => reader.GetDateTime();
        public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions o)
            => writer.WriteStringValue(DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("O", CultureInfo.InvariantCulture));
    }
}
