using System.Collections;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using Kusto.Data.Common;

namespace EFCore.Kusto.Storage;

public sealed class KustoDataReader : DbDataReader
{
    private readonly IDataReader _inner;
    private readonly ICslQueryProvider _client; // hold client

    public KustoDataReader(IDataReader inner, ICslQueryProvider client)
    {
        _inner = inner;
        _client = client;
    }

    public override void Close()
    {
        _inner.Close();
        _client.Dispose(); // close the client WHEN the reader is closed
    }

    public override bool Read() => _inner.Read();

    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        => Task.FromResult(Read());

    public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
        => Task.FromResult(GetFieldValue<T>(ordinal));

    public override T GetFieldValue<T>(int ordinal)
        => (T)_inner.GetValue(ordinal);

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
            return null;   // <--- THIS FIXES THE ODATA EXPAND BEHAVIOR

        return value;
    }

    public override int GetInt32(int ordinal) => _inner.GetInt32(ordinal);
    public override long GetInt64(int ordinal) => _inner.GetInt64(ordinal);
    public override double GetDouble(int ordinal) => ((SqlDouble)_inner.GetValue(ordinal)).Value;
    public override decimal GetDecimal(int ordinal) => ((SqlDecimal)_inner.GetValue(ordinal)).Value;
    public override DateTime GetDateTime(int ordinal) => _inner.GetDateTime(ordinal);
    public override bool GetBoolean(int ordinal) => _inner.GetBoolean(ordinal);
    public override float GetFloat(int ordinal) => (float)((SqlDecimal)_inner.GetValue(ordinal)).Value;
    public override short GetInt16(int ordinal) => ((SqlInt16)_inner.GetValue(ordinal)).Value;
    public override byte GetByte(int ordinal) => ((SqlByte)_inner.GetValue(ordinal)).Value;
    public override Guid GetGuid(int ordinal) => Guid.Parse(_inner.GetValue(ordinal).ToString());

    public override long GetBytes(int a, long b, byte[] c, int d, int e) => throw new NotSupportedException();

    public override char GetChar(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetChars(int a, long b, char[] c, int d, int e) => throw new NotSupportedException();

    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++) values[i] = _inner.GetValue(i);
        return count;
    }
}