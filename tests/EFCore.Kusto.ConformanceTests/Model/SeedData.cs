namespace EFCore.Kusto.ConformanceTests.Model;

/// <summary>
/// Deterministic seed data, held as an object graph with navigations wired up so the same LINQ query can run
/// in memory as the oracle. Strings include quotes, backslashes, newlines, non-ASCII and empty values.
/// </summary>
public sealed class SeedData : IShop
{
    public List<Customer> CustomerList { get; } = new();
    public List<Order> OrderList { get; } = new();
    public List<OrderLine> OrderLineList { get; } = new();
    public List<Product> ProductList { get; } = new();

    public IQueryable<Customer> Customers => CustomerList.AsQueryable();
    public IQueryable<Order> Orders => OrderList.AsQueryable();
    public IQueryable<OrderLine> OrderLines => OrderLineList.AsQueryable();
    public IQueryable<Product> Products => ProductList.AsQueryable();

    private static readonly string[] Cities = { "Lahore", "Karachi", "O\"Brien Town", "Zoë", "back\\slash", "" };
    private static readonly string[] Statuses = { "open", "shipped", "cancelled", "Open" };
    private static readonly string[] Categories = { "tools", "toys", "food", "line\nbreak" };

    // Declared after the arrays above: static fields initialize in declaration order, and Build reads them.
    public static readonly SeedData Instance = Build();

    private static SeedData Build()
    {
        var data = new SeedData();
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        for (var i = 1; i <= 15; i++)
        {
            data.ProductList.Add(new Product
            {
                Id = i,
                Name = $"Product {i}",
                Category = Categories[i % Categories.Length],
                Price = 1.25m * i,
                Weight = 0.5 * i,
                Ref = new Guid(i, 0, 0, new byte[8]),
                CreatedAt = start.AddDays(i),
            });
        }

        var lineId = 1L;
        for (var c = 1; c <= 12; c++)
        {
            var customer = new Customer
            {
                Id = c,
                Name = c == 3 ? "O\"Brien" : $"Customer {c}",
                City = Cities[c % Cities.Length],
                Code = $"C{c:000}",
                Tier = c % 3,
                IsActive = c % 4 != 0,
                CreatedAt = start.AddDays(c * 3).AddHours(c),
            };
            data.CustomerList.Add(customer);

            // Customers 11 and 12 have no orders, so left joins and APPLY see parents without children.
            var orderCount = c >= 11 ? 0 : (c % 5) + 1;
            for (var k = 0; k < orderCount; k++)
            {
                var orderId = c * 100 + k;
                var order = new Order
                {
                    Id = orderId,
                    CustomerId = c,
                    Customer = customer,
                    Name = $"Order {orderId}",
                    Code = k == 0 ? customer.Code : $"O{orderId}",
                    Status = Statuses[(c + k) % Statuses.Length],
                    Total = 10.5m * (k + 1) + c,
                    Discount = k % 3 == 0 ? null : 0.05 * k,
                    Priority = k % 2 == 0 ? k : null,
                    CreatedAt = customer.CreatedAt.AddDays(k + 1),
                };
                customer.Orders.Add(order);
                data.OrderList.Add(order);

                for (var l = 0; l < (k % 3) + 1; l++)
                {
                    var product = data.ProductList[(int)((orderId + l) % data.ProductList.Count)];
                    var line = new OrderLine
                    {
                        Id = lineId++,
                        OrderId = orderId,
                        Order = order,
                        ProductId = product.Id,
                        Product = product,
                        Quantity = l + 1,
                        Price = product.Price,
                        Note = l == 1 ? "fragile" : "",
                    };
                    order.Lines.Add(line);
                    data.OrderLineList.Add(line);
                }
            }
        }

        return data;
    }
}
