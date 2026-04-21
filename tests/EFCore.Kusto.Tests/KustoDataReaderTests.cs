using System;
using System.Data;
using System.Data.SqlTypes;
using System.Threading;
using System.Threading.Tasks;
using EFCore.Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Data;
using Kusto.Data.Results;
using Xunit;

namespace EFCore.Kusto.Tests;

public class KustoDataReaderTests
{
    [Fact]
    public void Byte_arrays_round_trip_and_base64_is_decoded()
    {
        using var inner = CreateInnerReader(
            ("NullBytes", typeof(string), null),
            ("Base64Bytes", typeof(string), Convert.ToBase64String(new byte[] { 1, 2, 3 })),
            ("RawBytes", typeof(string), Convert.ToBase64String(new byte[] { 9, 8, 7 })));

        using var reader = new KustoDataReader(inner, new FakeCslQueryProvider());

        reader.Read();

        Assert.Empty(reader.GetFieldValue<byte[]>(reader.GetOrdinal("NullBytes")));
        Assert.Equal(new byte[] { 1, 2, 3 }, reader.GetFieldValue<byte[]>(reader.GetOrdinal("Base64Bytes")));
        Assert.Equal(new byte[] { 9, 8, 7 }, reader.GetFieldValue<byte[]>(reader.GetOrdinal("RawBytes")));
    }

    [Fact]
    public void Typed_getters_match_Kusto_return_shapes_and_handle_failures()
    {
        var now = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);

        using var inner = CreateInnerReader(
            ("Double", typeof(double), 12.34),
            ("Decimal", typeof(SqlDecimal), 56.78m),
            ("Float", typeof(float), (float)1.5),
            ("Int16", typeof(short), (short)12),
            ("Byte", typeof(byte), (byte)5),
            ("Guid", typeof(Guid), "b8e1c2c5-4a4c-4e23-9b0a-7a84397c30d4"),
            ("DateTime", typeof(DateTime), now),
            ("DateTimeBad", typeof(string), "not-a-date"),
            ("EmptyString", typeof(string), string.Empty),
            ("Boolean", typeof(bool), true));

        using var reader = new KustoDataReader(inner, new FakeCslQueryProvider());

        reader.Read();

        Assert.Equal(12.34, reader.GetDouble(reader.GetOrdinal("Double")));
        Assert.InRange(reader.GetDecimal(reader.GetOrdinal("Decimal")), 56.779m, 56.781m);
        Assert.Equal(1.5f, reader.GetFloat(reader.GetOrdinal("Float")));
        Assert.Equal((short)12, reader.GetInt16(reader.GetOrdinal("Int16")));
        Assert.Equal((byte)5, reader.GetByte(reader.GetOrdinal("Byte")));
        Assert.Equal(Guid.Parse("b8e1c2c5-4a4c-4e23-9b0a-7a84397c30d4"), reader.GetGuid(reader.GetOrdinal("Guid")));
        Assert.Equal(now, reader.GetDateTime(reader.GetOrdinal("DateTime")));
        Assert.Equal(DateTime.MinValue, reader.GetDateTime(reader.GetOrdinal("DateTimeBad")));
        Assert.Equal(null, reader.GetString(reader.GetOrdinal("EmptyString")));
        Assert.True(reader.GetBoolean(reader.GetOrdinal("Boolean")));
    }

    [Fact]
    public void Close_disposes_underlying_provider()
    {
        using var inner = CreateInnerReader(("Dummy", typeof(int), 1));
        var provider = new FakeCslQueryProvider();

        using var reader = new KustoDataReader(inner, provider);
        reader.Close();

        Assert.True(provider.Disposed);
    }

    private static IDataReader CreateInnerReader(params (string Name, Type Type, object? Value)[] columns)
    {
        var table = new DataTable();
        foreach (var (name, type, _) in columns)
        {
            table.Columns.Add(name, type);
        }

        var row = table.NewRow();
        foreach (var (name, _, value) in columns)
        {
            row[name] = value ?? DBNull.Value;
        }

        table.Rows.Add(row);

        var sdkReader = KustoJsonDataStream.CreateReaderWriterPairForTest(
            table.CreateDataReader(),
            new KustoDataReaderOptions(),
            _ => { });

        return (sdkReader);
    }

    private sealed class FakeCslQueryProvider : ICslQueryProvider
    {
        public string DefaultDatabaseName { get; set; } = string.Empty;
        public bool Disposed { get; private set; }

        public void Dispose() => Disposed = true;

        public IDataReader ExecuteQuery(string query) => throw new NotImplementedException();

        public IDataReader ExecuteQuery(string query, ClientRequestProperties properties) =>
            throw new NotImplementedException();

        public IDataReader ExecuteQuery(string databaseName, string query, ClientRequestProperties properties) =>
            throw new NotImplementedException();

        public Task<IDataReader> ExecuteQueryAsync(
            string databaseName,
            string query,
            ClientRequestProperties properties,
            CancellationToken cancellationToken) => throw new NotImplementedException();

        public Task<ProgressiveDataSet> ExecuteQueryV2Async(
            string databaseName,
            string query,
            ClientRequestProperties properties,
            CancellationToken cancellationToken) => throw new NotImplementedException();
    }

    /// <summary>
    /// Wraps the Kusto SDK reader and surfaces Kusto-style Sql* values so KustoDataReader sees the same shapes as production.
    /// </summary>
    private sealed class SqlCoercingDataReader : IDataReader
    {
        private readonly DataTableReader _inner;

        public SqlCoercingDataReader(IDataReader reader)
        {
            _inner = new DataTableReader(ToDataTable(reader));
        }

        private static DataTable ToDataTable(IDataReader reader)
        {
            var table = new DataTable();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                table.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
            }

            while (reader.Read())
            {
                var values = new object[reader.FieldCount];
                reader.GetValues(values);
                table.Rows.Add(values);
            }

            return table;
        }

        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(GetOrdinal(name));
        public int Depth => _inner.Depth;
        public int FieldCount => _inner.FieldCount;
        public bool IsClosed => _inner.IsClosed;
        public int RecordsAffected => _inner.RecordsAffected;

        public void Close() => _inner.Close();
        public void Dispose() => _inner.Dispose();
        public bool GetBoolean(int i) => Convert.ToBoolean(GetValue(i));
        public byte GetByte(int i) => Convert.ToByte(GetValue(i));

        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) =>
            _inner.GetBytes(i, fieldOffset, buffer, bufferoffset, length);

        public char GetChar(int i) => _inner.GetChar(i);

        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) =>
            _inner.GetChars(i, fieldoffset, buffer, bufferoffset, length);

        public IDataReader GetData(int i) => _inner.GetData(i);
        public string GetDataTypeName(int i) => _inner.GetDataTypeName(i);
        public DateTime GetDateTime(int i) => Convert.ToDateTime(GetValue(i));
        public decimal GetDecimal(int i) => Convert.ToDecimal(GetValue(i));
        public double GetDouble(int i) => Convert.ToDouble(GetValue(i));
        public Type GetFieldType(int i) => _inner.GetFieldType(i);
        public float GetFloat(int i) => Convert.ToSingle(GetValue(i));

        public Guid GetGuid(int i)
        {
            var value = GetValue(i);
            return value switch
            {
                Guid g => g,
                string s => Guid.Parse(s),
                _ => Guid.Parse(value.ToString()!)
            };
        }

        public short GetInt16(int i) => Convert.ToInt16(GetValue(i));
        public int GetInt32(int i) => Convert.ToInt32(GetValue(i));
        public long GetInt64(int i) => Convert.ToInt64(GetValue(i));
        public string GetName(int i) => _inner.GetName(i);
        public int GetOrdinal(string name) => _inner.GetOrdinal(name);
        public DataTable GetSchemaTable() => _inner.GetSchemaTable();

        public string GetString(int i) => Convert.ToString(GetValue(i))!;

        public object GetValue(int i)
        {
            var value = _inner.GetValue(i);

            return value switch
            {
                double d => new System.Data.SqlTypes.SqlDouble(d),
                float f => new System.Data.SqlTypes.SqlDecimal((decimal)f),
                decimal m => new System.Data.SqlTypes.SqlDecimal(m),
                short s => new System.Data.SqlTypes.SqlInt16(s),
                byte b => new System.Data.SqlTypes.SqlByte(b),
                _ => value
            };
        }

        public int GetValues(object[] values)
        {
            var count = _inner.GetValues(values);
            for (int i = 0; i < count; i++)
            {
                values[i] = values[i] switch
                {
                    double d => new System.Data.SqlTypes.SqlDouble(d),
                    float f => new System.Data.SqlTypes.SqlDecimal((decimal)f),
                    decimal m => new System.Data.SqlTypes.SqlDecimal(m),
                    short s => new System.Data.SqlTypes.SqlInt16(s),
                    byte b => new System.Data.SqlTypes.SqlByte(b),
                    _ => values[i]
                };
            }

            return count;
        }

        public bool IsDBNull(int i) => _inner.IsDBNull(i);
        public bool NextResult() => _inner.NextResult();
        public bool Read() => _inner.Read();
    }
}