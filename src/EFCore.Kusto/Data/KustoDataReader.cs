using System.Collections;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using Kusto.Data.Common;

namespace EFCore.Kusto.Data;

/// <summary>
/// Wraps a Kusto <see cref="IDataReader"/> to present a <see cref="DbDataReader"/> surface for EF Core.
/// </summary>
public sealed class KustoDataReader : DbDataReader
{
    private readonly IDataReader _inner;
    private readonly ICslQueryProvider _client;

    /// <summary>
    /// Initializes a new instance of the <see cref="KustoDataReader"/> class.
    /// </summary>
    /// <param name="inner">The underlying Kusto data reader.</param>
    /// <param name="client">The Kusto query provider used to issue the command.</param>
    public KustoDataReader(IDataReader inner, ICslQueryProvider client)
    {
        _inner = inner;
        _client = client;
    }

    public override void Close()
    {
        _inner.Close();
        _client.Dispose();
    }

    public override bool Read() => _inner.Read();

    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        => Task.FromResult(Read());

    public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
        => Task.FromResult(GetFieldValue<T>(ordinal));

    public override T GetFieldValue<T>(int ordinal)
    {
        var value = _inner.GetValue(ordinal);

        if (typeof(T) == typeof(byte[]))
        {
            if (value == null)
                return (T)(object)Array.Empty<byte>();

            if (value is string s)
            {
                if (string.IsNullOrWhiteSpace(s))
                    return (T)(object)Array.Empty<byte>();

                return (T)(object)Convert.FromBase64String(s);
            }

            if (value is byte[] b)
                return (T)(object)b;

            return (T)(object)Array.Empty<byte>();
        }

        return (T)value;
    }


    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _inner.Dispose();

        base.Dispose(disposing);
    }


    public override int FieldCount => _inner.FieldCount;

    public override string GetName(int ordinal) => _inner.GetName(ordinal);
    public override int GetOrdinal(string name) => _inner.GetOrdinal(name);

    public override object GetValue(int ordinal) => _inner.GetValue(ordinal);
    public override bool IsDBNull(int ordinal) => _inner.IsDBNull(ordinal);

    public override bool HasRows => _inner is DbDataReader dr ? dr.HasRows : true;
    public override bool IsClosed => _inner.IsClosed;
    public override int RecordsAffected => 11;
    public override int Depth => _inner.Depth;

    public override object this[int ordinal] => _inner.GetValue(ordinal);
    public override object this[string name] => _inner.GetValue(_inner.GetOrdinal(name));

    public override IEnumerator GetEnumerator()
    {
        while (Read())
            yield return this;
    }

    public override bool NextResult() => _inner.NextResult();

    public override string GetDataTypeName(int ordinal) => _inner.GetDataTypeName(ordinal);
    public override Type GetFieldType(int ordinal) => _inner.GetFieldType(ordinal);

    public override string GetString(int ordinal)
    {
        var value = _inner.GetString(ordinal);

        if (string.IsNullOrEmpty(value))
            return null;

        return value;
    }
    
    public override DateTime GetDateTime(int ordinal)
    {
        try
        {
            return _inner.GetDateTime(ordinal);
        }
        catch (Exception)
        {
            return DateTime.MinValue;
        }
    }

    public override int GetInt32(int ordinal) => _inner.GetInt32(ordinal);
    public override long GetInt64(int ordinal) => _inner.GetInt64(ordinal);
    public override double GetDouble(int ordinal) => _inner.GetDouble(ordinal);
    public override decimal GetDecimal(int ordinal) => ((SqlDecimal)_inner.GetValue(ordinal)).Value;
    public override bool GetBoolean(int ordinal) => _inner.GetBoolean(ordinal);
    public override float GetFloat(int ordinal) => _inner.GetFloat(ordinal);
    public override short GetInt16(int ordinal) => _inner.GetInt16(ordinal);
    public override byte GetByte(int ordinal) => _inner.GetByte(ordinal);
    public override Guid GetGuid(int ordinal) => _inner.GetGuid(ordinal);

    public override long GetBytes(int a, long b, byte[] c, int d, int e) => _inner.GetBytes(a, b, c, d, e);

    public override char GetChar(int ordinal) => _inner.GetChar(ordinal);

    public override long GetChars(int a, long b, char[] c, int d, int e) => _inner.GetChars(a, b, c, d, e);

    public override int GetValues(object[] values) => _inner.GetValues(values);
}