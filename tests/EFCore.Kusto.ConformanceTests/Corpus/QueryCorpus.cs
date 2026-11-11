using System.Collections;
using EFCore.Kusto.ConformanceTests.Model;

namespace EFCore.Kusto.ConformanceTests.Corpus;

/// <summary>
/// One LINQ query, written once against <see cref="IShop"/> so it runs both through the provider and in memory.
/// </summary>
/// <param name="Name">Unique name, used as the test case name.</param>
/// <param name="Query">The query. Projects to plain values or anonymous types, never entities with navigations.</param>
/// <param name="Ordered">True when the query has a total ordering, so row order is compared too.</param>
/// <param name="KnownIssue">
/// Set when the provider is known to get this query wrong. The tests then expect a failure, and fail once the
/// query starts working so the marker gets removed.
/// </param>
public sealed record QueryCase(string Name, Func<IShop, IQueryable> Query, bool Ordered, string? KnownIssue = null)
{
    public override string ToString() => Name;

    public List<object?> Run(IShop shop) => ((IEnumerable)Query(shop)).Cast<object?>().ToList();
}

/// <summary>A query ending in a scalar operator (Count, Any, Sum...), which only exists as an executed result.</summary>
public sealed record ScalarCase(string Name, Func<IShop, object> Query, string? KnownIssue = null)
{
    public override string ToString() => Name;
}

public static class QueryCorpus
{
    public static IEnumerable<object[]> Queries => All.Select(c => new object[] { c });
    public static IEnumerable<object[]> Scalars => AllScalars.Select(c => new object[] { c });

    private static QueryCase Q(string name, Func<IShop, IQueryable> query, bool ordered = true, string? knownIssue = null)
        => new(name, query, ordered, knownIssue);

    public static readonly IReadOnlyList<QueryCase> All = BuildQueries();
    public static readonly IReadOnlyList<ScalarCase> AllScalars = BuildScalars();

    private static List<QueryCase> BuildQueries()
    {
        var city = "Karachi";
        var ids = new long[] { 2, 3, 5, 8 };
        var since = new DateTime(2025, 1, 15, 0, 0, 0, DateTimeKind.Utc);
        var top = 2;
        var skip = 3;

        return new List<QueryCase>
        {
            // Filtering
            Q("where_constant", s => s.Customers.Where(c => c.City == "Lahore").OrderBy(c => c.Id).Select(c => new { c.Id, c.Name })),
            Q("where_parameter", s => s.Customers.Where(c => c.City == city).OrderBy(c => c.Id).Select(c => c.Id)),
            Q("where_constant_with_quote", s => s.Customers.Where(c => c.Name == "O\"Brien").Select(c => c.Id)),
            Q("where_constant_with_backslash", s => s.Customers.Where(c => c.City == "back\\slash").OrderBy(c => c.Id).Select(c => c.Id)),
            Q("where_constant_non_ascii", s => s.Customers.Where(c => c.City == "Zoë").OrderBy(c => c.Id).Select(c => c.Id)),
            Q("where_empty_string", s => s.Customers.Where(c => c.City == "").OrderBy(c => c.Id).Select(c => c.Id)),
            Q("where_and_or_not", s => s.Customers.Where(c => (c.Tier == 1 || c.Tier == 2) && !c.IsActive).OrderBy(c => c.Id).Select(c => c.Id)),
            Q("where_bool_column", s => s.Customers.Where(c => c.IsActive).OrderBy(c => c.Id).Select(c => c.Id)),
            Q("where_nullable_is_null", s => s.Orders.Where(o => o.Discount == null).OrderBy(o => o.Id).Select(o => o.Id)),
            Q("where_nullable_not_null", s => s.Orders.Where(o => o.Priority != null).OrderBy(o => o.Id).Select(o => o.Id)),
            Q("where_nullable_compare", s => s.Orders.Where(o => o.Discount > 0.06).OrderBy(o => o.Id).Select(o => o.Id)),
            Q("where_in_list", s => s.Customers.Where(c => ids.Contains(c.Id)).OrderBy(c => c.Id).Select(c => c.Id)),
            Q("where_not_in_list", s => s.Customers.Where(c => !ids.Contains(c.Id)).OrderBy(c => c.Id).Select(c => c.Id)),
            Q("where_datetime", s => s.Orders.Where(o => o.CreatedAt >= since).OrderBy(o => o.Id).Select(o => o.Id)),
            Q("where_decimal", s => s.Orders.Where(o => o.Total > 25.5m).OrderBy(o => o.Id).Select(o => o.Id)),
            Q("where_guid", s => s.Products.Where(p => p.Ref == new Guid(4, 0, 0, new byte[8])).Select(p => p.Id),
                knownIssue: "#13 Guid constant written as a raw literal instead of guid(...)"),
            Q("where_string_contains", s => s.Orders.Where(o => o.Name.Contains("10")).OrderBy(o => o.Id).Select(o => o.Id)),
            Q("where_string_starts_ends", s => s.Orders.Where(o => o.Code.StartsWith("O") && o.Code.EndsWith("1")).OrderBy(o => o.Id).Select(o => o.Id)),
            Q("where_case_sensitive_equals", s => s.Orders.Where(o => o.Status == "open").OrderBy(o => o.Id).Select(o => o.Id)),

            // Projection
            Q("select_arithmetic", s => s.OrderLines.OrderBy(l => l.Id).Select(l => new { l.Id, Amount = l.Price * l.Quantity, Double = l.Quantity * 2 })),
            Q("select_conditional", s => s.Customers.OrderBy(c => c.Id).Select(c => new { c.Id, Level = c.Tier > 1 ? "gold" : "standard" })),
            Q("select_coalesce", s => s.Orders.OrderBy(o => o.Id).Select(o => new { o.Id, D = o.Discount ?? 0.0 }),
                knownIssue: "#14 ?? written as SQL COALESCE instead of coalesce"),
            Q("select_string_concat", s => s.Customers.OrderBy(c => c.Id).Select(c => new { c.Id, Label = c.Code + "-" + c.Name }),
                knownIssue: "#15 string + written as + instead of strcat"),
            Q("select_date_part", s => s.Orders.OrderBy(o => o.Id).Select(o => new { o.Id, o.CreatedAt.Year, o.CreatedAt.Month, o.CreatedAt.Day })),

            // Ordering and paging
            Q("order_by_multiple", s => s.Orders.OrderBy(o => o.CustomerId).ThenByDescending(o => o.Total).ThenBy(o => o.Id).Select(o => o.Id)),
            Q("skip_take", s => s.Orders.OrderBy(o => o.Id).Skip(skip).Take(top * 2).Select(o => o.Id)),
            Q("take_constant", s => s.Products.OrderByDescending(p => p.Price).ThenBy(p => p.Id).Take(3).Select(p => p.Id)),

            // Aggregation
            Q("group_by_count_sum", s => s.Orders.GroupBy(o => o.CustomerId)
                .Select(g => new { CustomerId = g.Key, Count = g.Count(), Sum = g.Sum(o => o.Total), Max = g.Max(o => o.Total) }), ordered: false),
            Q("group_by_two_keys", s => s.Orders.GroupBy(o => new { o.CustomerId, o.Status })
                .Select(g => new { g.Key.CustomerId, g.Key.Status, Count = g.Count() }), ordered: false),
            Q("group_by_having", s => s.Orders.GroupBy(o => o.Status).Where(g => g.Count() > 5).Select(g => g.Key), ordered: false),
            Q("distinct", s => s.Orders.Select(o => o.Status).Distinct(), ordered: false, knownIssue: "#10 Distinct() is dropped"),

            // Joins: Name, Code and CreatedAt exist on both sides.
            Q("join_inner_same_named_columns", s => s.Orders.Join(s.Customers, o => o.CustomerId, c => c.Id,
                    (o, c) => new { OrderId = o.Id, CustomerId = c.Id, OrderName = o.Name, CustomerName = c.Name, c.City, OrderCode = o.Code, CustomerCode = c.Code }),
                ordered: false, knownIssue: "#3 right side's same-named columns read the left side's"),
            Q("join_filter_on_right_same_named", s => s.Orders.Join(s.Customers, o => o.CustomerId, c => c.Id, (o, c) => new { o, c })
                    .Where(x => x.c.Name == "O\"Brien").Select(x => x.o.Id),
                ordered: false, knownIssue: "#3 right side's same-named columns read the left side's"),
            Q("join_on_non_key_same_named", s => s.Orders.Join(s.Customers, o => o.Code, c => c.Code, (o, c) => new { o.Id, CustomerId = c.Id }),
                ordered: false, knownIssue: "#3 right side's same-named columns read the left side's"),
            Q("left_join", s => s.Customers
                    .GroupJoin(s.Orders, c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
                    .SelectMany(x => x.os.DefaultIfEmpty(), (x, o) => new { CustomerId = x.c.Id, OrderId = o == null ? (long?)null : o.Id }),
                ordered: false, knownIssue: "#3 right side's same-named columns read the left side's"),
            Q("reference_navigation", s => s.Orders.Where(o => o.Customer!.City == "Lahore").OrderBy(o => o.Id).Select(o => new { o.Id, o.Customer!.Tier })),
            Q("two_hop_navigation", s => s.OrderLines.Where(l => l.Order!.Customer!.Tier == 2).OrderBy(l => l.Id).Select(l => new { l.Id, l.Product!.Category })),

            // Per-parent collections (APPLY and per-group limits)
            Q("collection_top_per_parent", s => s.Customers.OrderBy(c => c.Id)
                .Select(c => new { c.Id, Top = c.Orders.OrderByDescending(o => o.Total).ThenBy(o => o.Id).Take(2).Select(o => o.Id).ToList() }),
                knownIssue: "#23 row numbering skips its sort; also #3"),
            Q("collection_top_per_parent_paged", s => s.Customers.OrderBy(c => c.Id).Skip(2).Take(4)
                .Select(c => new { c.Id, Top = c.Orders.OrderBy(o => o.Id).Take(top).Select(o => new { o.Id, o.Total }).ToList() }),
                knownIssue: "#3 right side's same-named columns read the left side's"),
            Q("collection_filtered_parent", s => s.Customers.Where(c => c.Tier == 1).OrderBy(c => c.Id)
                .Select(c => new { c.Id, Top = c.Orders.OrderBy(o => o.Id).Take(1).Select(o => o.Id).ToList() }),
                knownIssue: "#3 right side's same-named columns read the left side's"),
            Q("collection_two_expands", s => s.Orders.OrderBy(o => o.Id).Take(5)
                .Select(o => new { o.Id, Lines = o.Lines.OrderBy(l => l.Id).Take(1).Select(l => l.Id).ToList(), o.Customer!.Name }),
                knownIssue: "#3 right side's same-named columns read the left side's"),
            Q("first_per_group", s => s.Orders.GroupBy(o => o.CustomerId).Select(g => g.OrderBy(o => o.Total).ThenBy(o => o.Id).First().Id), ordered: false,
                knownIssue: "#16 First() per group becomes a scalar subquery Kusto can't run"),
            Q("select_many_take", s => s.Customers.SelectMany(c => c.Orders.OrderBy(o => o.Id).Take(1)).Select(o => o.Id), ordered: false,
                knownIssue: "#3 right side's same-named columns read the left side's"),

            // Subqueries
            Q("where_any_children", s => s.Customers.Where(c => c.Orders.Any(o => o.Status == "shipped")).OrderBy(c => c.Id).Select(c => c.Id),
                knownIssue: "#9 Any() inside Where produces invalid KQL"),
            Q("where_contains_subquery", s => s.Customers.Where(c => s.Orders.Where(o => o.Total > 40m).Select(o => o.CustomerId).Contains(c.Id))
                .OrderBy(c => c.Id).Select(c => c.Id), knownIssue: "#12 Contains over a subquery crashes"),
        };
    }

    private static List<ScalarCase> BuildScalars()
    {
        var city = "Lahore";
        return new List<ScalarCase>
        {
            new("count_all", s => s.Orders.Count()),
            new("count_filtered", s => s.Orders.Count(o => o.Status == "open")),
            new("any_true", s => s.Customers.Any(c => c.City == city)),
            new("any_false", s => s.Customers.Any(c => c.City == "Nowhere"), KnownIssue: "#24 Any() that should be false throws"),
            new("sum", s => s.OrderLines.Sum(l => l.Quantity)),
            new("max_decimal", s => s.Orders.Max(o => o.Total)),
            new("min_datetime", s => s.Orders.Min(o => o.CreatedAt)),
        };
    }
}
