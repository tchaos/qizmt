<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Static Methods of mstring #



## `Prepare` ##
`public static mstring Prepare()`

Returns an empty mstring.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    int i1 = rKey.GetInt32();
    int i2 = rKey.GetInt32();

    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        Int64 i3 = rValue.GetInt64();
        mstring s = rValue.GetString();
        int i4 = rValue.GetInt32();
        mstring delimiter = mstring.Prepare(",");

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




`public static mstring Prepare(string s)`

Encodes string s using UTF-16 encoding and returns an instance of msting with this encoded string data.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    int i1 = rKey.GetInt32();
    int i2 = rKey.GetInt32();

    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        Int64 i3 = rValue.GetInt64();
        mstring s = rValue.GetString();
        int i4 = rValue.GetInt32();
        mstring delimiter = mstring.Prepare(",");

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




`public static mstring Prepare(ByteSlice b)`

Converts the buffer of the `ByteSlice` from UTF-8 encoding to UTF-16 encoding.  Creates an instance of mstring with this data.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,s,i
    //61,92,383,Washington,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    mstring s = ms.CsvNextItemToString();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString(s);
    rValue.PutInt32(i4);

    output.Add(rKey, rValue);
} 
```

---




`public static mstring Prepare(double x)`

Converts the double x to mstring.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring sCoords = sLine.MSubstring(2);

    double x = sCoords.NextItemToDouble(',');
    double y = sCoords.NextItemToDouble(',');

    double kx = x * (double)5;
    double ky = y * (double)5;

    kx = Math.Truncate(kx);
    ky = Math.Truncate(ky);

    mstring sKey = mstring.Prepare(kx);
    sKey = sKey.MAppend(',').MAppend(ky);

    output.Add(sKey, sLine);
} 
```

---




`public static mstring Prepare(Int16 x)`

Returns an mstring representation of Int16 x.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int16 id = sLine.NextItemToInt16(',');
    mstring name = sLine.NextItemToString(',').MToLower();
    Int16 x = sLine.NextItemToInt16(',');

    Int16 total = id + x;

    mstring key = mstring.Prepare(total);

    recordset rs = recordset.Prepare();
    rs.PutString(name);
    rs.PutInt64(x);

    output.Add(key, rs);
} 
```

---




`public static mstring Prepare(UInt16 x)`

Returns an mstring representation of UInt16 x.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt16 id = sLine.NextItemToUInt16(',');
    mstring name = sLine.NextItemToString(',').MToLower();
    UInt16 x = sLine.NextItemToUInt16(',');

    UInt16 total = id + x;

    mstring key = mstring.Prepare(total);

    recordset rs = recordset.Prepare();
    rs.PutString(name);
    rs.PutInt64(x);

    output.Add(key, rs);
} 
```

---




`public static mstring Prepare(char c)`

Converts the char c to mstring.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    int i1 = rKey.GetInt32();
    int i2 = rKey.GetInt32();

    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        Int64 i3 = rValue.GetInt64();
        mstring s = rValue.GetString();
        int i4 = rValue.GetInt32();
        mstring delimiter = mstring.Prepare(',');

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




`public static mstring Prepare(Int32 x)`

Returns an mstring representation of Int32 x.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int32 id = sLine.CsvNextItemToInt32();
    mstring name = sLine.CsvNextItemToString().MToLower();
    Int32 x = sLine.CsvNextItemToInt32();

    Int32 total = id + x;

    mstring key = mstring.Prepare(total);

    recordset rs = recordset.Prepare();
    rs.PutString(name);
    rs.PutInt64(x);

    output.Add(key, rs);
} 
```

---




`public static mstring Prepare(UInt32 x)`

Returns an mstring representation of UInt32 x.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt32 id = sLine.NextItemToUInt32(',');
    mstring name = sLine.NextItemToString(',').MToLower();
    UInt32 x = sLine.NextItemToUInt32(',');

    UInt32 total = id + x;

    mstring key = mstring.Prepare(total);

    recordset rs = recordset.Prepare();
    rs.PutString(name);
    rs.PutInt64(x);

    output.Add(key, rs);
} 
```

---




`public static mstring Prepare(Int64 x)`

Returns an mstring representation of Int64 x.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int64 id = sLine.CsvNextItemToInt64();
    mstring name = sLine.CsvNextItemToString().MToLower();
    Int64 x = sLine.CsvNextItemToInt64();

    Int64 total = id + x;

    mstring key = mstring.Prepare(total);

    recordset rs = recordset.Prepare();
    rs.PutString(name);
    rs.PutInt64(x);

    output.Add(key, rs);
} 
```

---




`public static mstring Prepare(UInt64 x)`

Returns an mstring representation of UInt64 x.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt64 id = sLine.NextItemToUInt64(',');
    mstring name = sLine.CsvNextItemToString(',').MToLower();
    UInt64 x = sLine.NextItemToUInt64(',');

    UInt64 total = id + x;

    mstring key = mstring.Prepare(total);

    recordset rs = recordset.Prepare();
    rs.PutString(name);
    rs.PutInt64(x);

    output.Add(key, rs);
} 
```

---




## `Copy` ##
`public static mstring Copy(mstring s)`

Returns a copy of mstring s.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    mstring sKey = ms.MSubstring(0, 2);
    mstring sCode = mstring.Prepare("xb");
    mstring sValue;

    if (sKey == sCode)
    {
        sValue = mstring.Copy(sKey);
        sValue.MAppend(mstring.Prepare("000"));
    }
    else
    {
        sValue = mstring.Prepare();
    }

    output.Add(sKey.ToByteSlice(), sValue.ToByteSlice());
} 
```