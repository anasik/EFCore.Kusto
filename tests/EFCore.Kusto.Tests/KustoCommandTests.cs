using System.Data;
using EFCore.Kusto.Data;
using Xunit;

namespace EFCore.Kusto.Tests;

public class KustoCommandTests
{
    [Theory]
    [InlineData(DbType.AnsiString, "string")]
    [InlineData(DbType.String, "string")]
    [InlineData(DbType.StringFixedLength, "string")]
    [InlineData(DbType.AnsiStringFixedLength, "string")]
    [InlineData(DbType.Byte, "int")]
    [InlineData(DbType.SByte, "int")]
    [InlineData(DbType.Int16, "int")]
    [InlineData(DbType.UInt16, "int")]
    [InlineData(DbType.Int32, "int")]
    [InlineData(DbType.Int64, "long")]
    [InlineData(DbType.UInt32, "long")]
    [InlineData(DbType.UInt64, "long")]
    [InlineData(DbType.Boolean, "bool")]
    [InlineData(DbType.DateTime, "datetime")]
    [InlineData(DbType.DateTime2, "datetime")]
    [InlineData(DbType.DateTimeOffset, "datetime")]
    [InlineData(DbType.Date, "datetime")]
    [InlineData(DbType.Time, "timespan")]
    [InlineData(DbType.Decimal, "decimal")]
    [InlineData(DbType.Double, "real")]
    [InlineData(DbType.Single, "real")]
    [InlineData(DbType.Guid, "guid")]
    [InlineData(DbType.Object, "string")]
    public void GetKustoType_maps_DbType_to_Kusto_scalar_type(DbType dbType, string expectedKustoType)
    {
        Assert.Equal(expectedKustoType, KustoCommand.GetKustoType(dbType));
    }

    [Fact]
    public void GetKustoType_maps_Decimal_distinctly_from_Double_and_Single()
    {
        // Regression guard: DbType.Decimal used to be lumped in with Double/Single as "real"
        // (a binary floating type), which silently loses precision for exact decimal values.
        // It must map to Kusto's own "decimal" type instead.
        Assert.Equal("decimal", KustoCommand.GetKustoType(DbType.Decimal));
        Assert.NotEqual(KustoCommand.GetKustoType(DbType.Decimal), KustoCommand.GetKustoType(DbType.Double));
    }
}
