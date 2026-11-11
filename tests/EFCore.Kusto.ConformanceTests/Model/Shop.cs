using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore;

namespace EFCore.Kusto.ConformanceTests.Model;

// The conformance model deliberately reuses column names across tables (Id, Name, Code, CreatedAt) so joins
// exercise same-named columns, and mixes every mapped CLR type, nullable numbers and awkward strings.

public sealed class Customer
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public string City { get; set; } = "";
    public string Code { get; set; } = "";
    public int Tier { get; set; }
    public bool IsActive { get; set; }
    public DateTime CreatedAt { get; set; }
    public List<Order> Orders { get; set; } = new();
}

public sealed class Order
{
    public long Id { get; set; }
    public long CustomerId { get; set; }
    public string Name { get; set; } = "";
    public string Code { get; set; } = "";
    public string Status { get; set; } = "";
    public decimal Total { get; set; }
    public double? Discount { get; set; }
    public int? Priority { get; set; }
    public DateTime CreatedAt { get; set; }
    public Customer? Customer { get; set; }
    public List<OrderLine> Lines { get; set; } = new();
}

public sealed class OrderLine
{
    public long Id { get; set; }
    public long OrderId { get; set; }
    public long ProductId { get; set; }
    public int Quantity { get; set; }
    public decimal Price { get; set; }
    public string Note { get; set; } = "";
    public Order? Order { get; set; }
    public Product? Product { get; set; }
}

public sealed class Product
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public string Category { get; set; } = "";
    public decimal Price { get; set; }
    public double Weight { get; set; }
    public Guid Ref { get; set; }
    public DateTime CreatedAt { get; set; }
}

/// <summary>The data a corpus query runs against: EF sets for the provider, plain lists for the in-memory oracle.</summary>
public interface IShop
{
    IQueryable<Customer> Customers { get; }
    IQueryable<Order> Orders { get; }
    IQueryable<OrderLine> OrderLines { get; }
    IQueryable<Product> Products { get; }
}

public sealed class ShopContext(DbContextOptions<ShopContext> options) : DbContext(options), IShop
{
    public DbSet<Customer> CustomerSet => Set<Customer>();
    public DbSet<Order> OrderSet => Set<Order>();
    public DbSet<OrderLine> OrderLineSet => Set<OrderLine>();
    public DbSet<Product> ProductSet => Set<Product>();

    IQueryable<Customer> IShop.Customers => CustomerSet.AsNoTracking();
    IQueryable<Order> IShop.Orders => OrderSet.AsNoTracking();
    IQueryable<OrderLine> IShop.OrderLines => OrderLineSet.AsNoTracking();
    IQueryable<Product> IShop.Products => ProductSet.AsNoTracking();

    public static ShopContext Create(string clusterUrl, string database, Action<EFCore.Kusto.Infrastructure.KustoDbContextOptionsBuilder>? kusto = null)
        => new(new DbContextOptionsBuilder<ShopContext>().UseKusto(clusterUrl, database, kusto).Options);

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<Customer>(b =>
        {
            b.ToTable("Customers");
            b.HasKey(c => c.Id);
            b.HasMany(c => c.Orders).WithOne(o => o.Customer).HasForeignKey(o => o.CustomerId);
        });
        mb.Entity<Order>(b =>
        {
            b.ToTable("Orders");
            b.HasKey(o => o.Id);
            b.HasMany(o => o.Lines).WithOne(l => l.Order).HasForeignKey(l => l.OrderId);
        });
        mb.Entity<OrderLine>(b =>
        {
            b.ToTable("OrderLines");
            b.HasKey(l => l.Id);
            b.HasOne(l => l.Product).WithMany().HasForeignKey(l => l.ProductId);
        });
        mb.Entity<Product>(b =>
        {
            b.ToTable("Products");
            b.HasKey(p => p.Id);
        });
    }
}
