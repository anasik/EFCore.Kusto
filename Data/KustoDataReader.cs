using System.Collections;
using System.Data;
using System.Data.Common;

namespace EFCore.Kusto;

public sealed class KustoDataReader : DbDataReader
{
    private readonly IDataReader _inner;

    public KustoDataReader(IDataReader inner)
    {
        _inner = inner;
    }

    // ------------------------------------------------------------
    // BASIC READER CONTROLS
    // ------------------------------------------------------------
    public override bool Read() => _inner.Read();

    public override bool NextResult() => _inner.NextResult();

    public override int Depth => 0;

    public override bool IsClosed => false;

    public override int RecordsAffected => 0;

    public override bool HasRows => true; // Kusto doesn't expose this; assume true

    // ------------------------------------------------------------
    // METADATA
    // ------------------------------------------------------------
    public override int FieldCount => _inner.FieldCount;

    public override string GetName(int ordinal) => _inner.GetName(ordinal);

    public override int GetOrdinal(string name) => _inner.GetOrdinal(name);

    public override string GetDataTypeName(int ordinal)
        => _inner.GetDataTypeName(ordinal);

    public override Type GetFieldType(int ordinal)
        => _inner.GetFieldType(ordinal);

    // ------------------------------------------------------------
    // FIELD GETTERS
    // ------------------------------------------------------------
    public override object GetValue(int ordinal) => _inner.GetValue(ordinal);

    public override bool IsDBNull(int ordinal) => _inner.IsDBNull(ordinal);

    public override int GetInt32(int ordinal) => (int)_inner.GetValue(ordinal);

    public override long GetInt64(int ordinal) => (long)_inner.GetValue(ordinal);

    public override short GetInt16(int ordinal) => (short)_inner.GetValue(ordinal);

    public override bool GetBoolean(int ordinal) => (bool)_inner.GetValue(ordinal);

    public override byte GetByte(int ordinal) => (byte)_inner.GetValue(ordinal);

    public override char GetChar(int ordinal) => (char)_inner.GetValue(ordinal);

    public override Guid GetGuid(int ordinal) => (Guid)_inner.GetValue(ordinal);

    public override float GetFloat(int ordinal) => (float)_inner.GetValue(ordinal);

    public override double GetDouble(int ordinal) => (double)_inner.GetValue(ordinal);

    public override decimal GetDecimal(int ordinal) => (decimal)_inner.GetValue(ordinal);

    public override DateTime GetDateTime(int ordinal) => (DateTime)_inner.GetValue(ordinal);

    public override string GetString(int ordinal) => (string)_inner.GetValue(ordinal);

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        byte[] data = (byte[])_inner.GetValue(ordinal);

        int available = data.Length - (int)dataOffset;
        int toCopy = Math.Min(length, Math.Max(0, available));

        if (buffer != null)
            Array.Copy(data, dataOffset, buffer, bufferOffset, toCopy);

        return toCopy;
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        char[] data = ((string)_inner.GetValue(ordinal)).ToCharArray();

        int available = data.Length - (int)dataOffset;
        int toCopy = Math.Min(length, Math.Max(0, available));

        if (buffer != null)
            Array.Copy(data, dataOffset, buffer, bufferOffset, toCopy);

        return toCopy;
    }

    // ------------------------------------------------------------
    // BULK GETTER
    // ------------------------------------------------------------
    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, _inner.FieldCount);

        for (int i = 0; i < count; i++)
            values[i] = _inner.GetValue(i);

        return count;
    }

    // ------------------------------------------------------------
    // INDEXERS
    // ------------------------------------------------------------
    public override object this[int ordinal] => _inner.GetValue(ordinal);

    public override object this[string name] => _inner.GetValue(_inner.GetOrdinal(name));

    // ------------------------------------------------------------
    // SCHEMA — EF DOES NOT REQUIRE A SCHEMA TABLE
    // ------------------------------------------------------------
    public override System.Data.DataTable? GetSchemaTable() => null;

    // ------------------------------------------------------------
    // ENUMERATOR SUPPORT
    // ------------------------------------------------------------
    public override IEnumerator GetEnumerator()
    {
        while (Read())
        {
            object[] row = new object[_inner.FieldCount];
            GetValues(row);
            yield return row;
        }
    }
}
