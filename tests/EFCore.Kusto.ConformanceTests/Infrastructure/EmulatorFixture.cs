using EFCore.Kusto.ConformanceTests.Model;
using EFCore.Kusto.Extensions;
using EFCore.Kusto.Storage;
using Kusto.Data;
using Kusto.Data.Net.Client;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.Kusto.ConformanceTests.Infrastructure;

/// <summary>
/// Loads <see cref="SeedData"/> into a local Kusto emulator (the <c>kustainer-linux</c> container). Set
/// <c>KUSTO_EMULATOR_URL</c> (e.g. <c>http://localhost:8080</c>) to enable; without it the emulator tests skip.
/// </summary>
public sealed class EmulatorFixture : IDisposable
{
    public const string UrlVariable = "KUSTO_EMULATOR_URL";
    public const string Database = "NetDefaultDB";

    public string? Url { get; } = Environment.GetEnvironmentVariable(UrlVariable);
    public string? SkipReason { get; }

    public EmulatorFixture()
    {
        if (string.IsNullOrWhiteSpace(Url))
        {
            SkipReason = $"{UrlVariable} is not set; start the Kusto emulator to run these tests.";
            return;
        }

        using var context = CreateContext();
        using var admin = KustoClientFactory.CreateCslAdminProvider(new KustoConnectionStringBuilder(Url) { InitialCatalog = Database });
        foreach (var entity in context.Model.GetEntityTypes())
        {
            var table = entity.GetTableName()!;
            var columns = Columns(entity);
            var schema = string.Join(", ", columns.Select(c => $"['{c.Name}']:{c.Type}"));
            var rows = Rows(entity.ClrType).Select(row => string.Join(", ",
                columns.Select(c => KustoLiteral.Format(c.Property.PropertyInfo!.GetValue(row), c.Type))));

            admin.ExecuteControlCommand($".drop table ['{table}'] ifexists");
            admin.ExecuteControlCommand($".create table ['{table}'] ({schema})");
            admin.ExecuteControlCommand(
                $".set-or-append ['{table}'] <| datatable({schema}) [\n{string.Join(",\n", rows)}\n]");
        }
    }

    public ShopContext CreateContext() => ShopContext.Create(Url!, Database, kusto => kusto.UseNoAuthentication());

    private static List<(string Name, string Type, IProperty Property)> Columns(IEntityType entity)
    {
        var table = StoreObjectIdentifier.Table(entity.GetTableName()!, entity.GetSchema());
        return entity.GetProperties()
            .Select(p => (p.GetColumnName(table)!, p.GetRelationalTypeMapping().StoreType, p))
            .ToList();
    }

    private static IEnumerable<object> Rows(Type clrType) =>
        clrType == typeof(Customer) ? SeedData.Instance.CustomerList
        : clrType == typeof(Order) ? SeedData.Instance.OrderList
        : clrType == typeof(OrderLine) ? SeedData.Instance.OrderLineList
        : clrType == typeof(Product) ? SeedData.Instance.ProductList
        : throw new InvalidOperationException($"No seed data for {clrType.Name}");

    public void Dispose() { }
}
