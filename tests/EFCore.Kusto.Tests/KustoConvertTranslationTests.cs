using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Linq.Expressions;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EFCore.Kusto.Tests;

/// <summary>
/// EF emits SQL <c>CAST(x AS type)</c> for type conversions; Kusto has no CAST, so every
/// conversion must come out as the matching <c>to&lt;type&gt;(x)</c> function (or vanish
/// when source and target store types are identical).
/// </summary>
public class KustoConvertTranslationTests
{
    private sealed class Row
    {
        public long Id { get; set; }
        public string? S { get; set; }
        public decimal D { get; set; }
        public decimal? DN { get; set; }
        public int I { get; set; }
        public long L { get; set; }
        public DateTime Dt { get; set; }
    }

    private sealed class Ctx(DbContextOptions<Ctx> options) : DbContext(options)
    {
        public DbSet<Row> Rows => Set<Row>();

        /// <summary>
        /// Test-only function whose translation wraps its argument in a SQL-level
        /// <c>Convert</c> to the same store type. EF folds such conversions out of
        /// plain LINQ, but a provider can still receive one (OData's aggregate binder
        /// does on the live API: <c>CAST(ClosePrice AS decimal)</c> over a decimal column).
        /// </summary>
        public static decimal? SameTypeConvert(decimal? value) => throw new NotSupportedException();

        public static DateOnly ToDateOnly(DateTime value) => throw new NotSupportedException();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.HasDbFunction(typeof(Ctx).GetMethod(nameof(ToDateOnly))!)
                .HasTranslation(args => new SqlUnaryExpression(
                    ExpressionType.Convert, args[0], typeof(DateOnly), new DateOnlyTypeMapping("date")));
            mb.Entity<Row>().ToTable("Rows");
            mb.HasDbFunction(typeof(Ctx).GetMethod(nameof(SameTypeConvert))!)
                .HasTranslation(args => new SqlUnaryExpression(
                    ExpressionType.Convert, args[0], typeof(decimal), args[0].TypeMapping));
        }
    }

    private static Ctx CreateContext()
        => new(new DbContextOptionsBuilder<Ctx>().UseKusto("https://example.westus.kusto.windows.net", "Db").Options);

    private static string Kql(IQueryable query) => Normalize(query.ToQueryString());

    [Fact]
    public void Aggregate_over_int_converted_to_decimal_uses_todecimal()
    {
        using var ctx = CreateContext();
        var q = ctx.Rows.GroupBy(r => r.S).Select(g => new { g.Key, T = g.Sum(r => (decimal)r.I) });

        Assert.Equal(
            "Rows\n| summarize T = sum(todecimal(I)) by S\n| project Key = S, T", Kql(q));
    }

    [Fact]
    public void Average_over_int_converted_to_decimal_uses_todecimal()
    {
        using var ctx = CreateContext();
        var q = ctx.Rows.GroupBy(r => r.S).Select(g => new { g.Key, T = g.Average(r => (decimal?)r.I) });

        Assert.Equal(
            "Rows\n| summarize T = avg(todecimal(I)) by S\n| project Key = S, T", Kql(q));
    }

    [Fact]
    public void Aggregate_over_decimal_converted_to_double_uses_toreal()
    {
        using var ctx = CreateContext();
        var q = ctx.Rows.GroupBy(r => r.S).Select(g => new { g.Key, T = g.Sum(r => (double)r.D) });

        Assert.Equal(
            "Rows\n| summarize T = sum(toreal(D)) by S\n| project Key = S, T", Kql(q));
    }

    [Fact]
    public void Aggregate_over_decimal_column_with_same_store_type_conversion_emits_no_cast()
    {
        using var ctx = CreateContext();
        var q = ctx.Rows.GroupBy(r => r.S).Select(g => new { g.Key, T = g.Sum(r => Ctx.SameTypeConvert(r.DN)) });

        Assert.Equal(
            "Rows\n| summarize T = sum(DN) by S\n| project Key = S, T", Kql(q));
    }

    [Fact]
    public void Same_store_type_conversion_in_where_and_projection_emits_no_cast()
    {
        using var ctx = CreateContext();
        var q = ctx.Rows.Where(r => Ctx.SameTypeConvert(r.DN) > 1m).Select(r => new { V = Ctx.SameTypeConvert(r.DN) });

        var kql = Kql(q);
        Assert.DoesNotContain("CAST", kql);
        Assert.DoesNotContain("todecimal", kql);
    }

    [Fact]
    public void Conversion_from_datetime_to_date_is_a_no_op()
    {
        using var ctx = CreateContext();
        var q = ctx.Rows.Select(r => new { V = Ctx.ToDateOnly(r.Dt) });

        // datetime -> date is the same KQL type, so no conversion function is emitted.
        Assert.Equal("Rows\n| project V = Dt", Kql(q));
    }

    [Fact]
    public void No_key_aggregate_with_conversion()
    {
        using var ctx = CreateContext();
        var q = ctx.Rows.GroupBy(r => 1).Select(g => new { T = g.Sum(r => (decimal)r.I) });

        Assert.Equal(
            "(\nRows\n| project I = I, Key = 1\n)\n| summarize T = sum(todecimal(I)) by Key\n| project T", Kql(q));
    }

    [Fact]
    public void Where_clause_conversion()
    {
        using var ctx = CreateContext();
        var q = ctx.Rows.Where(r => (decimal)r.I > 5m);

        var kql = Kql(q);
        Assert.DoesNotContain("CAST", kql);
        Assert.Contains("todecimal(I)", kql);
    }

    [Fact]
    public void Projection_conversions()
    {
        using var ctx = CreateContext();
        var q = ctx.Rows.Select(r => new { A = (long)r.I, B = (double)r.L, C = (decimal)r.L });

        var kql = Kql(q);
        Assert.DoesNotContain("CAST", kql);
        Assert.Contains("tolong(I)", kql);
        Assert.Contains("toreal(L)", kql);
        Assert.Contains("todecimal(L)", kql);
    }

    [Fact]
    public void Order_by_aggregate_with_conversion()
    {
        using var ctx = CreateContext();
        var q = ctx.Rows.GroupBy(r => r.S)
            .Select(g => new { g.Key, T = g.Sum(r => (decimal)r.I) })
            .OrderByDescending(x => x.T);

        var kql = Kql(q);
        Assert.DoesNotContain("CAST", kql);
        Assert.Contains("| summarize T = sum(todecimal(I)) by S", kql);
        Assert.Contains("| order by T desc", kql);
    }

    private static string Normalize(string queryString)
    {
        var lines = queryString.Replace("\r\n", "\n").Split('\n');
        var values = new Dictionary<string, string>();
        int bodyStart = 0;
        for (int i = 0; i < lines.Length; i++)
        {
            var m = Regex.Match(lines[i], @"^--\s+(\S+)='(.*)'$");
            if (!m.Success) { bodyStart = i; break; }
            values[m.Groups[1].Value] = m.Groups[2].Value;
            bodyStart = i + 1;
        }
        var body = string.Join("\n", lines.Skip(bodyStart));
        foreach (var kv in values)
            body = Regex.Replace(body, @"\b" + Regex.Escape(kv.Key) + @"\b", kv.Value);
        return body;
    }
}
