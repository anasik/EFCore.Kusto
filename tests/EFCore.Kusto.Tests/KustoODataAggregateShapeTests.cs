using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.Kusto.Tests;

/// <summary>
/// OData <c>$apply</c> builds each result row as a linked list of
/// <c>{ Name = "alias", Value = expr, Next = ... }</c> containers, so EF hands the
/// generator repeated <c>Name</c>/<c>Value</c> aliases and constant projections.
/// These tests pin the KQL emitted for that shape.
/// </summary>
public class KustoODataAggregateShapeTests
{
    private sealed class Container
    {
        public string Name { get; set; } = "";
        public object? Value { get; set; }
        public Container? Next { get; set; }
    }

    private sealed class Wrapper { public Container? C { get; set; } }

    private sealed class Prop
    {
        public long Id { get; set; }
        public string City { get; set; } = "";
        public string? StandardStatus { get; set; }
        public decimal? ClosePrice { get; set; }
    }

    private sealed class Ctx(DbContextOptions<Ctx> options) : DbContext(options)
    {
        public DbSet<Prop> Property => Set<Prop>();
        protected override void OnModelCreating(ModelBuilder mb) => mb.Entity<Prop>().ToTable("Property");
    }

    private static Ctx CreateContext()
        => new(new DbContextOptionsBuilder<Ctx>().UseKusto("https://example.westus.kusto.windows.net", "Db").Options);

    [Fact]
    public void Single_aggregate()
    {
        using var ctx = CreateContext();
        var query = ctx.Property.GroupBy(p => 1)
            .Select(g => new Wrapper { C = new Container { Name = "S", Value = g.Sum(p => p.ClosePrice) } });

        Assert.Equal(
            "(\nProperty\n| project ClosePrice = ClosePrice, Key = 1\n)\n" +
            "| summarize Value = sum(ClosePrice) by Key\n" +
            "| project Name = \"S\", Value",
            Normalize(query.ToQueryString()));
    }

    [Fact]
    public void Single_average()
    {
        using var ctx = CreateContext();
        var query = ctx.Property.GroupBy(p => 1)
            .Select(g => new Wrapper { C = new Container { Name = "A", Value = g.Average(p => p.ClosePrice) } });

        Assert.Equal(
            "(\nProperty\n| project ClosePrice = ClosePrice, Key = 1\n)\n" +
            "| summarize Value = avg(ClosePrice) by Key\n" +
            "| project Name = \"A\", Value",
            Normalize(query.ToQueryString()));
    }

    [Fact]
    public void Multiple_aggregates_get_unique_summarize_and_project_aliases()
    {
        using var ctx = CreateContext();
        var query = ctx.Property.GroupBy(p => 1).Select(g => new Wrapper
        {
            C = new Container
            {
                Name = "S", Value = g.Sum(p => p.ClosePrice),
                Next = new Container
                {
                    Name = "N", Value = g.Count(),
                    Next = new Container { Name = "A", Value = g.Average(p => p.ClosePrice) }
                }
            }
        });

        Assert.Equal(
            "(\nProperty\n| project ClosePrice = ClosePrice, Key = 1\n)\n" +
            "| summarize Value = sum(ClosePrice), Value_1 = count(), Value_2 = avg(ClosePrice) by Key\n" +
            "| project Name = \"S\", Value, Name_1 = \"N\", Value_1, Name_2 = \"A\", Value_2",
            Normalize(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_key_with_aggregate_does_not_put_constants_in_summarize()
    {
        using var ctx = CreateContext();
        var query = ctx.Property.GroupBy(p => p.StandardStatus).Select(g => new Wrapper
        {
            C = new Container
            {
                Name = "StandardStatus", Value = g.Key,
                Next = new Container { Name = "S", Value = g.Sum(p => p.ClosePrice) }
            }
        });

        Assert.Equal(
            "Property\n" +
            "| summarize Value_1 = sum(ClosePrice) by StandardStatus\n" +
            "| project Name = \"StandardStatus\", Value = StandardStatus, Name_1 = \"S\", Value_1",
            Normalize(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_key_with_count()
    {
        using var ctx = CreateContext();
        var query = ctx.Property.GroupBy(p => p.City).Select(g => new Wrapper
        {
            C = new Container
            {
                Name = "City", Value = g.Key,
                Next = new Container { Name = "C", Value = g.Count() }
            }
        });

        Assert.Equal(
            "Property\n" +
            "| summarize Value_1 = count() by City\n" +
            "| project Name = \"City\", Value = City, Name_1 = \"C\", Value_1",
            Normalize(query.ToQueryString()));
    }

    [Fact]
    public void Filtered_groupby_with_multiple_aggregates()
    {
        using var ctx = CreateContext();
        var query = ctx.Property.Where(p => p.StandardStatus == "Closed")
            .GroupBy(p => p.StandardStatus).Select(g => new Wrapper
            {
                C = new Container
                {
                    Name = "StandardStatus", Value = g.Key,
                    Next = new Container
                    {
                        Name = "S", Value = g.Sum(p => p.ClosePrice),
                        Next = new Container { Name = "A", Value = g.Average(p => p.ClosePrice) }
                    }
                }
            });

        Assert.Equal(
            "Property\n| where StandardStatus == \"Closed\"\n" +
            "| summarize Value_1 = sum(ClosePrice), Value_2 = avg(ClosePrice) by StandardStatus\n" +
            "| project Name = \"StandardStatus\", Value = StandardStatus, Name_1 = \"S\", Value_1, Name_2 = \"A\", Value_2",
            Normalize(query.ToQueryString()));
    }

    [Fact]
    public void GroupBy_aggregate_with_take()
    {
        using var ctx = CreateContext();
        var query = ctx.Property.GroupBy(p => p.StandardStatus).Select(g => new Wrapper
        {
            C = new Container
            {
                Name = "StandardStatus", Value = g.Key,
                Next = new Container { Name = "S", Value = g.Sum(p => p.ClosePrice) }
            }
        }).Take(5);

        Assert.Equal(
            "Property\n" +
            "| summarize Value_1 = sum(ClosePrice) by StandardStatus\n" +
            "| project Name = \"StandardStatus\", Value = StandardStatus, Name_1 = \"S\", Value_1\n" +
            "| take 5",
            Normalize(query.ToQueryString()));
    }

    [Fact]
    public void Ordering_by_aggregate_uses_resolved_alias()
    {
        using var ctx = CreateContext();
        var query = ctx.Property.GroupBy(p => p.City)
            .OrderBy(g => g.Max(p => p.ClosePrice))
            .Select(g => new Wrapper
            {
                C = new Container
                {
                    Name = "S", Value = g.Sum(p => p.ClosePrice),
                    Next = new Container { Name = "M", Value = g.Max(p => p.ClosePrice) }
                }
            });

        var kql = Normalize(query.ToQueryString());
        Assert.Contains("| summarize Value = sum(ClosePrice), Value_1 = max(ClosePrice) by City", kql);
        Assert.Contains("| order by Value_1 asc", kql);
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
