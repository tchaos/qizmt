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

---




# Non-static Methods of mstring #



## `ToByteSlice` ##
`public ByteSlice ToByteSlice()`

Converts the buffer of mstring to UTF-8 and returns a new instance of `ByteSlice`.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);
    mstring sKey = ms.MSubstring(0, 2);
    output.Add(sKey.ToByteSlice(), ByteSlice.Prepare());
} 
```

---




## `ToString` ##
`public override string ToString()`

Returns a C# string.
#### Remarks ####
This results a heap allocation.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);
    Qizmt_Log(ms.ToString());
} 
```

---




## `MSubstring` ##
`public mstring MSubstring(int startIndex, int length)`

Returns an instance of mstring starting at startIndex, with length number of characters.
#### Remarks ####
remarks

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    mstring sKey = ms.MSubstring(0, 2);

    output.Add(sKey.ToByteSlice(), ByteSlice.Prepare());
} 
```

---




`public mstring MSubstring(int startIndex)`

Returns an instance of mstring starting at startIndex, until the end of the mstring.
#### Remarks ####
remarks

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    mstring sKey = ms.MSubstring(2);

    output.Add(sKey.ToByteSlice(), ByteSlice.Prepare());
} 
```

---




## `SubstringM` ##
`public mstring SubstringM(int startIndex, int length)`

Returns an instance of mstring starting at startIndex, with length number of characters.
#### Remarks ####
This is equivalent to `MSubstring(int startIndex, int length)`.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);
    mstring sKey = ms.SubstringM(0, 2);
    output.Add(sKey.ToByteSlice(), ByteSlice.Prepare());
} 
```

---




`public mstring SubstringM(int startIndex)`

Returns an instance of mstring starting at startIndex, until the end of the mstring.
#### Remarks ####
This is equivalent to `MSubstring(int startIndex)`.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    mstring sKey = ms.SubstringM(2);

    output.Add(sKey.ToByteSlice(), ByteSlice.Prepare());
} 
```

---




## `MTrim` ##
`public mstring MTrim()`

Trims off spaces from the left and right of this mstring.  The mstring is changed here.  The mstring itself is returned.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstringarray parts = sLine.SplitM(' ');

    for (int i = 0; i < parts.Length; i++)
    {
        mstring word = parts[i].MTrim().MTrim(',').MTrim('!').MTrim('?').MTrim(':').MTrim(';').MTrim('(').MTrim(')');

        if (word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
        {
            output.Add(word.ToLowerM(), mstring.Prepare(1));
        }
    }
} 
```

---




`public mstring MTrim(char c)`

Trims off char c from the left and right of this mstring.  The mstring is changed here.  The mstring itself is returned.
#### Remarks ####
remarks

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstringarray parts = sLine.SplitM(' ');

    for (int i = 0; i < parts.Length; i++)
    {
        mstring word = parts[i].MTrim().MTrim(',').MTrim('!').MTrim('?').MTrim(':').MTrim(';').MTrim('(').MTrim(')');

        if (word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
        {
            output.Add(word.ToLowerM(), mstring.Prepare(1));
        }
    }
} 
```

---




## `TrimM` ##
`public mstring TrimM()`

Trims off spaces from the left and right of this mstring.  The mstring is changed here.  The mstring itself is returned.
#### Remarks ####
This is equivalent to `Mtrim()`.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstringarray parts = sLine.SplitM(' ');

    for (int i = 0; i < parts.Length; i++)
    {
        mstring word = parts[i].TrimM().TrimM(',').TrimM('!').TrimM('?').TrimM(':').TrimM(';').TrimM('(').TrimM(')');

        if (word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
        {
            output.Add(word.ToLowerM(), mstring.Prepare(1));
        }
    }
} 
```

---




`public mstring TrimM(char c)`

Trims off char c from the left and right of this mstring.  The mstring is changed here.  The mstring itself is returned.
#### Remarks ####
This is equivalent to Mtrim(char c)

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstringarray parts = sLine.SplitM(' ');

    for (int i = 0; i < parts.Length; i++)
    {
        mstring word = parts[i].TrimM().TrimM(',').TrimM('!').TrimM('?').TrimM(':').TrimM(';').TrimM('(').TrimM(')');

        if (word.Length > 0 && word.Length <= 16) // Word cannot be longer than the KeyLength!
        {
            output.Add(word.ToLowerM(), mstring.Prepare(1));
        }
    }
} 
```

---




## `MAppend` ##
`public mstring MAppend(mstring s)`

Appends mstring s.

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

---




`public mstring MAppend(char c)`

Appends the char c to this mstring.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);
    int dimensionality = rKey.GetInt32();
    long rightangles = rKey.GetInt64();
    int edges = rKey.GetInt32();
    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring shape = rValue.GetString();
        int corners = rValue.GetInt32();
        mstring line = mstring.Prepare();
        line = line.MAppend(dimensionality)
            .MAppend(',')
            .MAppend(rightangles)
            .MAppend(',')
            .MAppend(edges)
            .MAppend(',')
            .MAppend(shape)
            .MAppend(',')
            .MAppend(corners);
        output.Add(line);
    }
} 
```

---




`public mstring MAppend(Int16 x)`

Appends the Int16 x to this mstring.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring name = rValue.GetString();
        Int16 num = rValue.GetInt16();
        mstring line = mstring.Prepare();
        line = line.MAppend(name)
            .MAppend(',')
            .MAppend(num);
        output.Add(line);
    }
} 
```

---




`public mstring MAppend(double x)`

Appends the double x to this mstring.

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




`public mstring MAppend(UInt16 x)`

Appends the UInt16 x to this mstring.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring name = rValue.GetString();
        UInt16 num = rValue.GetUInt16();
        mstring line = mstring.Prepare();
        line = line.MAppend(name)
            .MAppend(',')
            .MAppend(num);
        output.Add(line);
    }
} 
```

---




`public mstring MAppend(Int32 x)`

Appends the Int32 x to this mstring.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);
    int dimensionality = rKey.GetInt32();
    long rightangles = rKey.GetInt64();
    int edges = rKey.GetInt32();
    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring shape = rValue.GetString();
        int corners = rValue.GetInt32();
        mstring line = mstring.Prepare();
        line = line.MAppend(dimensionality)
            .MAppend(',')
            .MAppend(rightangles)
            .MAppend(',')
            .MAppend(edges)
            .MAppend(',')
            .MAppend(shape)
            .MAppend(',')
            .MAppend(corners);
        output.Add(line);
    }
} 
```

---




`public mstring MAppend(UInt32 x)`

Appends the UInt32 x to this mstring.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);
    Uint32 dimensionality = rKey.GetUInt32();
    long rightangles = rKey.GetInt64();
    int edges = rKey.GetInt32();
    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring shape = rValue.GetString();
        int corners = rValue.GetInt32();
        mstring line = mstring.Prepare();
        line = line.MAppend(dimensionality)
            .MAppend(',')
            .MAppend(rightangles)
            .MAppend(',')
            .MAppend(edges)
            .MAppend(',')
            .MAppend(shape)
            .MAppend(',')
            .MAppend(corners);
        output.Add(line);
    }
} 
```

---




`public mstring MAppend(Int64 x)`

Appends the Int64 x to this mstring.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring name = rValue.GetString();
        Int64 num = rValue.GetInt64();
        mstring line = mstring.Prepare();
        line = line.MAppend(name)
            .MAppend(',')
            .MAppend(num);
        output.Add(line);
    }
} 
```

---




`public mstring MAppend(UInt64 x)`

Appends the UInt64 x to this mstring.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring name = rValue.GetString();
        UInt64 num = rValue.GetUInt64();
        mstring line = mstring.Prepare();
        line = line.MAppend(name)
            .MAppend(',')
            .MAppend(num);
        output.Add(line);
    }
} 
```

---




`public mstring MAppend(string s)`

Appends the string s to this mstring.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring sKey = mstring.SubstringM(0, 4);
    sKey.MAppend("++");

    output.Add(sKey, sLine);
} 
```

---




## `AppendM` ##
`public mstring AppendM(mstring s)`

Appends mstring s.
#### Remarks ####
This is equivalent to MAppend(mstring s).

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
        sValue.AppendM(mstring.Prepare("000"));
    }
    else
    {
        sValue = mstring.Prepare();
    }

    output.Add(sKey.ToByteSlice(), sValue.ToByteSlice());
} 
```

---




`public mstring AppendM(char c)`

Appends the char c to this mstring.
#### Remarks ####
This is equivalent to MAppend(char c).

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);
    int dimensionality = rKey.GetInt32();
    long rightangles = rKey.GetInt64();
    int edges = rKey.GetInt32();
    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring shape = rValue.GetString();
        int corners = rValue.GetInt32();
        mstring line = mstring.Prepare();
        line = line.AppendM(dimensionality)
            .AppendM(',')
            .AppendM(rightangles)
            .AppendM(',')
            .AppendM(edges)
            .AppendM(',')
            .AppendM(shape)
            .AppendM(',')
            .AppendM(corners);
        output.Add(line);
    }
} 
```

---




`public mstring AppendM(double x)`

Appends the double x to this mstring.
#### Remarks ####
This is equivalent to MAppend(double x).

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
    sKey = sKey.MAppend(',').AppendM(ky);

    output.Add(sKey, sLine);
} 
```

---




`public mstring AppendM(Int16 x)`

Appends the Int16 x to this mstring.
#### Remarks ####
This is equivalent to MAppend(Int16 x).

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring name = rValue.GetString();
        Int16 num = rValue.GetInt16();
        mstring line = mstring.Prepare();
        line = line.AppendM(name)
            .AppendM(',')
            .AppendM(num);
        output.Add(line);
    }
}
 
```

---




`public mstring AppendM(UInt16 x)`

Appends the UInt16 x to this mstring.
#### Remarks ####
This is equivalent to MAppend(UInt16 x).

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring name = rValue.GetString();
        UInt16 num = rValue.GetIntU16();
        mstring line = mstring.Prepare();
        line = line.AppendM(name)
            .AppendM(',')
            .AppendM(num);
        output.Add(line);
    }
} 
```

---




`public mstring AppendM(Int32 x)`

Appends the Int32 x to this mstring.
#### Remarks ####
This is equivalent to MAppend(Int32 x).

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);
    int dimensionality = rKey.GetInt32();
    long rightangles = rKey.GetInt64();
    int edges = rKey.GetInt32();
    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring shape = rValue.GetString();
        int corners = rValue.GetInt32();
        mstring line = mstring.Prepare();
        line = line.AppendM(dimensionality)
            .AppendM(',')
            .AppendM(rightangles)
            .AppendM(',')
            .AppendM(edges)
            .AppendM(',')
            .AppendM(shape)
            .AppendM(',')
            .AppendM(corners);
        output.Add(line);
    }
} 
```

---




`public mstring AppendM(UInt32 x)`

Appends the UInt32 x to this mstring.
#### Remarks ####
This is equivalent to MAppend(UInt32 x).

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);
    Uint32 dimensionality = rKey.GetUInt32();
    long rightangles = rKey.GetInt64();
    int edges = rKey.GetInt32();
    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring shape = rValue.GetString();
        int corners = rValue.GetInt32();
        mstring line = mstring.Prepare();
        line = line.AppendM(dimensionality)
            .AppendM(',')
            .AppendM(rightangles)
            .AppendM(',')
            .AppendM(edges)
            .AppendM(',')
            .AppendM(shape)
            .AppendM(',')
            .AppendM(corners);
        output.Add(line);
    }
} 
```

---




`public mstring AppendM(Int64 x)`

Appends the Int64 x to this mstring.
#### Remarks ####
This is equivalent to MAppend(Int64 x).

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring name = rValue.GetString();
        Int64 num = rValue.GetInt64();
        mstring line = mstring.Prepare();
        line = line.AppendM(name)
            .AppendM(',')
            .AppendM(num);
        output.Add(line);
    }
} 
```

---




`public mstring AppendM(UInt64 x)`

Appends the UInt64 x to this mstring.
#### Remarks ####
This is equivalent to MAppend(UInt64 x).

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        mstring name = rValue.GetString();
        UInt64 num = rValue.GetUInt64();
        mstring line = mstring.Prepare();
        line = line.AppendM(name)
            .AppendM(',')
            .AppendM(num);
        output.Add(line);
    }
}
</source>
|}



{|border="1" width="100%"
|-
|'''public mstring AppendM(string s)'''

Appends the string s to this mstring.
|-
|'''Remarks'''

This is equivalent to MAppend(string s).
|-
|'''Example'''
<source lang="csharp">
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring sKey = mstring.SubstringM(0, 4);
    sKey.AppendM("++");

    output.Add(sKey, sLine);
} 
```

---




## `Consume` ##
`public void Consume(ref mstring s)`

Appends mstring s and destroys s.

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




`public mstring Consume(ref char c)`

Appends char c and destroys c by setting c to '\0'.

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
        char delimiter = ',';

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




`public mstring Consume(ref double x)`

Appends double x and destroys x by setting x to 0.

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
        double x = rvalue.GetDouble();
        char delimiter = ',';
        ms.Consume(ref x);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




`public mstring Consume(ref Int16 x)`

Appends Int16 x and destroys x by setting x to 0.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        Int16 x = rvalue.GetInt16();
        char delimiter = ',';
        ms.Consume(ref x);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




`public mstring Consume(ref UInt16 x)`

Appends UInt16 x and destroys x by setting x to 0.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);
        UInt16 x = rvalue.GetUInt16();
        char delimiter = ',';
        ms.Consume(ref x);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




`public mstring Consume(ref Int32 x)`

Appends Int32 x and destroys x by setting x to 0.

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
        int x = rvalue.GetInt32();
        char delimiter = ',';
        ms.Consume(ref x);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




`public mstring Consume(ref UInt32 x)`

Appends UInt32 x and destroys x by setting x to 0.

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
        Uint32 x = rvalue.GetUInt32();
        char delimiter = ',';
        ms.Consume(ref x);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




`public mstring Consume(ref Int64 x)`

Appends Int64 x and destroys x by setting x to 0.

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
        Int64 x = rvalue.GetInt64();
        char delimiter = ',';
        ms.Consume(ref x);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




`public mstring Consume(ref UInt64 x)`

Appends UInt64 x and destroys x by setting x to 0.

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
        UInt64 x = rvalue.GetUInt64();
        char delimiter = ',';
        ms.Consume(ref x);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




`public mstring Consume(ref string s)`

Appends string s and destroys s by setting s to null.

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
        Int64 x = rvalue.GetInt64();
        string delimiter = "**";
        ms.Consume(ref x);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




## `CsvNextItemToString` ##
`public mstring CsvNextItemToString()`

Get the next Csv string item.

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




## `CsvNextItemToInt32` ##
`public Int32 CsvNextItemToInt32()`

Get the next Csv Int32 item.

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




## `CsvNextItemToInt` ##
`public Int32 CsvNextItemToInt()`

Get the next Csv Int32 item.
#### Remarks ####
This is equivalent to CsvNextItemToInt32().

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,s,i
    //61,92,383,Washington,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt();
    int i2 = ms.CsvNextItemToInt();
    Int64 i3 = ms.CsvNextItemToInt64();
    mstring s = ms.CsvNextItemToString();
    int i4 = ms.CsvNextItemToInt();

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




## `CsvNextItemToInt64` ##
`public Int32 CsvNextItemToInt64()`

Get the next Csv Int64 item.

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




## `CsvNextItemToLong` ##
`public Int64 CsvNextItemToLong()`

Get the next Csv Int64 item.
#### Remarks ####
This is equivalent to CsvNextItemToInt64().

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,s,i
    //61,92,383,Washington,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToLong();
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




## `CsvNextItemToDouble` ##
`public double CsvNextItemToDouble()`

Get the next Csv double item.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring sCoords = sLine.MSubstring(2);

    double x = sCoords.CsvNextItemToDouble();
    double y = sCoords.CsvNextItemToDouble();

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




## `NextItemToString` ##
`public mstring NextItemToString(char delimiter)`

Get the next string item, delimited by the char delimiter.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    int i1 = sLine.NextItemToInt32('|');
    mstring value = sLine.NextItemToString('|');
    int i2 = sLine.NextItemToInt32('|');

    int total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToInt16` ##
`public Int32 NextItemToInt16(char delimiter)`

Get the next Int16 item, delimited by the char delimiter.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int16 i1 = sLine.NextItemToInt16('|');
    mstring value = sLine.NextItemToString('|');
    Int16 i2 = sLine.NextItemToInt16('|');

    Int16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToIntShort` ##
`public Int32 NextItemToIntShort(char delimiter)`

Get the next Int16 item, delimited by the char delimiter.
#### Remarks ####
This is equivalent to NextItemToInt16.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int16 i1 = sLine.NextItemToShort('|');
    mstring value = sLine.NextItemToString('|');
    Int16 i2 = sLine.NextItemToShort('|');

    Int16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToUInt16` ##
`public Int32 NextItemToUInt16(char delimiter)`

Get the next UInt16 item, delimited by the char delimiter.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt16 i1 = sLine.NextItemToUInt16('|');
    mstring value = sLine.NextItemToString('|');
    UInt16 i2 = sLine.NextItemToUInt16('|');

    UInt16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToUShort` ##
`public Int32 NextItemToUShort(char delimiter)`

Get the next UInt16 item, delimited by the char delimiter.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt16 i1 = sLine.NextItemToUShort('|');
    mstring value = sLine.NextItemToString('|');
    UInt16 i2 = sLine.NextItemToUShort('|');

    UInt16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToInt32` ##
`public Int32 NextItemToInt32(char delimiter)`

Get the next Int32 item, delimited by the char delimiter.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    int i1 = sLine.NextItemToInt32('|');
    mstring value = sLine.NextItemToString('|');
    int i2 = sLine.NextItemToInt32('|');

    int total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToInt` ##
`public Int32 NextItemToInt(char delimiter)`

Get the next Int32 item, delimited by the char delimiter.
#### Remarks ####
This is equivalent to NextItemToInt32(char delimiter).

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    int i1 = sLine.NextItemToInt('|');
    mstring value = sLine.NextItemToString('|');
    int i2 = sLine.NextItemToInt('|');

    int total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToUInt32` ##
`public Int32 NextItemToUInt32(char delimiter)`

Get the next UInt32 item, delimited by the char delimiter.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt32 i1 = sLine.NextItemToUInt32('|');
    mstring value = sLine.NextItemToString('|');
    UInt32 i2 = sLine.NextItemToUInt32('|');

    UInt32 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToUInt` ##
`public Int32 NextItemToUInt(char delimiter)`

Get the next UInt32 item, delimited by the char delimiter.
#### Remarks ####
This is equivalent to NextItemToUInt32.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt32 i1 = sLine.NextItemToUInt('|');
    mstring value = sLine.NextItemToString('|');
    UInt32 i2 = sLine.NextItemToUInt('|');

    UInt32 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToLong` ##
`public Int64 NextItemToLong(char delimiter)`

Get the next Int64 item, delimited by the char delimiter.
#### Remarks ####
This is equivalent to NextItemToInt64(char delimiter).

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int64 i1 = sLine.NextItemToLong('|');
    mstring value = sLine.NextItemToString('|');
    Int64 i2 = sLine.NextItemToLong('|');

    Int64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToInt64` ##
`public Int64 NextItemToInt64(char delimiter)`

Get the next Int64 item, delimited by the char delimiter.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int64 i1 = sLine.NextItemToInt64('|');
    mstring value = sLine.NextItemToString('|');
    Int64 i2 = sLine.NextItemToInt64('|');

    Int64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToUInt64` ##
`public Int64 NextItemToUInt64(char delimiter)`

Get the next UInt64 item, delimited by the char delimiter.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt64 i1 = sLine.NextItemToUInt64('|');
    mstring value = sLine.NextItemToString('|');
    UInt64 i2 = sLine.NextItemToUInt64('|');

    UInt64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToULong` ##
`public Int64 NextItemToULong(char delimiter)`

Get the next UInt64 item, delimited by the char delimiter.
#### Remarks ####
This is equivalent to NextItemToUInt64.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    UInt64 i1 = sLine.NextItemToULong('|');
    mstring value = sLine.NextItemToString('|');
    UInt64 i2 = sLine.NextItemToULong('|');

    UInt64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `NextItemToDouble` ##
`public double NextItemToDouble(char delimiter)`

Get the next double item, delimited by the char delimiter.

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




## `ResetGetPosition` ##
`public void ResetGetPosition()`

Reset the get position to the very first beginning.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int64 i1 = sLine.NextItemToInt64('|');
    Int64 i2 = sLine.NextItemToInt64('|');
    Int64 total = i1 + i2;

    mstring key = mstring.Prepare(total);
    mstring value = GetValue(sline);

    output.Add(key, value);
}

private mstring GetValue(mstring sline)
{
    sline.ResetGetPosition();
    Int64 num = sline.NextItemToInt64('|');
    num = sline.NextItemToInt64('|');
    mstring name = sline.NextItemToString('|');
    mstring id = sline.NextItemToString('|');
    name = name.AppendM(id);
    return name;
} 
```

---




## `MToUpper` ##
`public mstring MToUpper()`

Returns a copy of this mstring converted to uppercase.

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
    rValue.PutString(s.MToUpper());
    rValue.PutInt32(i4);

    output.Add(rKey, rValue);

    Qizmt_Log("key = " + rKey.ToString());
    Qizmt_Log("value = " + rValue.ToString());
} 
```

---




## `ToUpperM` ##
`public mstring ToUpperM()`

Returns a copy of this mstring converted to uppercase.
#### Remarks ####
This is equivalent to MToUpper().

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
    rValue.PutString(s.ToUpperM());
    rValue.PutInt32(i4);

    output.Add(rKey, rValue);

    Qizmt_Log("key = " + rKey.ToString());
    Qizmt_Log("value = " + rValue.ToString());
} 
```

---




## `MToLower` ##
`public mstring MToLower()`

Returns a copy of this mstring converted to lowercase.

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
    rValue.PutString(s.MToLower());
    rValue.PutInt32(i4);

    output.Add(rKey, rValue);

    Qizmt_Log("key = " + rKey.ToString());
    Qizmt_Log("value = " + rValue.ToString());
} 
```

---




## `ToLowerM` ##
`public mstring ToLowerM()`

Returns a copy of this mstring converted to lowercase.
#### Remarks ####
This is equivalent to MToLower().

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
    rValue.PutString(s.ToLowerM());
    rValue.PutInt32(i4);

    output.Add(rKey, rValue);

    Qizmt_Log("key = " + rKey.ToString());
    Qizmt_Log("value = " + rValue.ToString());
} 
```

---




## `ToInt16` ##
`public Int16 ToInt16()`

Converts this mstring to Int16.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    Int16 i1 = parts[0].ToInt16();
    mstring value = parts[1];
    Int16 i2 = parts[2].ToInt16();

    Int16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
}
 
```

---




## `ToShort` ##
`public Int16 ToShort()`

Converts this mstring to Int16.
#### Remarks ####
This is equivalent to ToInt16.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    Int16 i1 = parts[0].ToShort();
    mstring value = parts[1];
    Int16 i2 = parts[2].ToShort();

    Int16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `ToUInt16` ##
`public UInt16 ToUInt16()`

Converts this mstring to UInt16.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt16 i1 = parts[0].ToUInt16();
    mstring value = parts[1];
    UInt16 i2 = parts[2].ToUInt16();

    UInt16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `ToUShort` ##
`public UInt16 ToUShort()`

Converts this mstring to UInt16.
#### Remarks ####
This is equivalent to ToUInt16.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt16 i1 = parts[0].ToUShort();
    mstring value = parts[1];
    UInt16 i2 = parts[2].ToUShort();

    UInt16 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `ToInt32` ##
`public Int32 ToInt32()`

Converts this mstring to Int32.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    int i1 = parts[0].ToInt32();
    mstring value = parts[1];
    int i2 = parts[2].ToInt32();

    int total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `ToInt` ##
`public Int32 ToInt()`

Converts this mstring to Int32.
#### Remarks ####
This is equivalent to ToInt32().

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    int i1 = parts[0].ToInt();
    mstring value = parts[1];
    int i2 = parts[2].ToInt();

    int total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
}
 
```

---




## `ToUInt32` ##
`public UInt32 ToUInt32()`

Converts this mstring to UInt32.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt32 i1 = parts[0].ToUInt32();
    mstring value = parts[1];
    UInt32 i2 = parts[2].ToUInt32();

    UInt32 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `ToUInt` ##
`public UInt32 ToUInt()`

Converts this mstring to UInt32.
#### Remarks ####
This is equivalent to ToUInt32.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt32 i1 = parts[0].ToUInt();
    mstring value = parts[1];
    UInt32 i2 = parts[2].ToUInt();

    UInt32 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `ToInt64` ##
`public Int64 ToInt64()`

Converts this mstring to Int64.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    Int64 i1 = parts[0].ToInt64();
    mstring value = parts[1];
    Int64 i2 = parts[2].ToInt64();

    Int64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `ToLong` ##
`public Int64 ToLong()`

Converts this mstring to Int64.
#### Remarks ####
This is equivalent to ToInt64().

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    Int64 i1 = parts[0].ToLong();
    mstring value = parts[1];
    Int64 i2 = parts[2].ToLong();

    Int64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `ToUInt64` ##
`public UInt64 ToUInt64()`

Converts this mstring to UInt64.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt64 i1 = parts[0].ToUInt64();
    mstring value = parts[1];
    UInt64 i2 = parts[2].ToUInt64();

    UInt64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
}
 
```

---




## `ToULong` ##
`public UInt64 ToULong()`

Converts this mstring to UInt64.
#### Remarks ####
This is equivalent to ToUInt64.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    UInt64 i1 = parts[0].ToULong();
    mstring value = parts[1];
    UInt64 i2 = parts[2].ToULong();

    UInt64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `ToDouble` ##
`public double ToDouble()`

Converts this mstring to double.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    double x = sLine.ToDouble() + 0.01;

    output.Add(mstring.Prepare(x), mstring.Prepare());
}    
```

---




## `MSplit` ##
`public mstringarray MSplit(char delimiter)`

Returns a mstring array that contains the substrings in this instance that are delimited by the char delimiter.

#### Example ####
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    mstringarray parts = sLine.MSplit('|');

    Int64 i1 = parts[0].ToInt64();
    mstring value = parts[1];
    Int64 i2 = parts[2].ToInt64();

    Int64 total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```

---




## `HasNextItem` ##
`public bool HasNextItem()`

Returns true if there is a next item in this mstring.

#### Example ####
```
/*
Sample Input:

Sam,1498,321,45,7,8,9,0
Jolie,1,2,3,4,5,6,7,8,9,0,1,2,3,4
John,61,42
May,1
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    int sum = 0;

    while (sLine.HasNextItem())
    {
        int amt = sLine.NextItemToInt(',');
        sum += amt;

    }

    recordset rValue = recordset.Prepare();
    rValue.PutInt(sum);

    output.Add(name, rValue);
} 
```

---




## `StartsWith` ##
`public bool StartsWith(mstring s)`

Returns true if this mstring starts with the mstring s.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring M = mstring.Prepare("M");

    if (name.StartsWith(M))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public bool StartsWith(string s)`

Returns true if this mstring starts with the string s.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');

    if (name.StartsWith("M"))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




## `Contains` ##
`public bool Contains(char c)`

Returns true if this mstring contains the char c.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');

    if (name.Contains('M'))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public bool Contains(mstring s)`

Returns true if this mstring contains the mstring s.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring M = mstring.Prepare("M.");

    if (name.Contains(M))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public bool Contains(string s)`

Returns true if this mstring contains the string s.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring M = mstring.Prepare("M.");

    if (name.Contains(M))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




## `EndsWith` ##
`public bool EndsWith(mstring s)`

Returns true if this mstring ends with the mstring s.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring er = mstring.Prepare("er");

    if (name.EndsWith(er))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public bool EndsWith(string s)`

Returns true if this mstring ends with the string s.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');

    if (name.EndsWith("er"))
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




## `IndexOf` ##
`public int IndexOf(string s)`

Returns the index of string s in this mstring.  If the string s is not found in this mstring, -1 is returned.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');

    if (name.IndexOf(" de ") > -1)
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public int IndexOf(mstring s)`

Returns the index of mstring s in this mstring.  If the mstring s is not found in this mstring, -1 is returned.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring de = mstring.Prepare(" de ");

    if (name.IndexOf(de) > -1)
    {
        output.Add(name, mstring.Prepare());
    }
} 
```

---




`public int IndexOf(char c)`

Returns the index of char c in this mstring.  If the char is not found, -1 is returned.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstringarray parts = sLine.SplitM(' ');

    for (int i = 0; i < parts.Length; i++)
    {
        mstring word = parts[i].TrimM('.').TrimM(',').TrimM('!').TrimM('?').TrimM(':').TrimM(';').TrimM('(').TrimM(')');

        if (word.Length > 0 && word.Length <= 16 && word.IndexOf('.') == 0)
        {
            output.Add(word.ToLowerM(), mstring.Prepare(1));
        }
    }
} 
```

---




## `MPadRight` ##
`public mstring MPadRight(int totalLength, char paddingChar)`

Pads this mstring on the right with the padding char for a specified total length.  This mstring is changed and returned.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring paddedName = name.MPadRight(16, ' ');
    output.Add(paddedName, mstring.Prepare());
} 
```

---




## `PadRightM` ##
`public mstring PadRightM(int totalLength, char paddingChar)`

Pads this mstring on the right with the padding char for a specified total length.  This mstring is changed and returned.
#### Remarks ####
This is equivalent to MPadRight.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring paddedName = name.PadRightM(16, ' ');
    output.Add(paddedName, mstring.Prepare());
} 
```

---




## `MPadLeft` ##
`public mstring MPadLeft(int totalLength, char paddingChar)`

Pads this mstring on the left with the padding char for a specified total length.  This mstring is changed and returned.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring paddedName = name.MPadLeft(16, ' ');
    output.Add(paddedName, mstring.Prepare());
} 
```

---




## `PadLeftM` ##
`public mstring PadLeftM(int totalLength, char paddingChar)`

Pads this mstring on the left with the padding char for a specified total length.  This mstring is changed and returned.

#### Remarks ####
This is equivalent to MPadLeft.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring paddedName = name.PadLeftM(16, ' ');
    output.Add(paddedName, mstring.Prepare());
} 
```

---




## `MReplace` ##
`public mstring MReplace(char oldChar, char newChar)`

Replace all occurances of old char with the new char.  This mstring is changed and returned.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newName = name.MReplace('.', '-');
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring MReplace(ref mstring oldString, ref mstring newString)`

Replace all occurances of old mstring with the new mstring.  This mstring is changed and returned.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring oldS = mstring.Prepare("Mr.");
    mstring newS = mstring.Prepare("");
    mstring newName = name.MReplace(ref oldS, ref newS);
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring MReplace(ref mstring oldString, string newString)`

Replace all occurances of old mstring with the new string.  This mstring is changed and returned.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring oldS = mstring.Prepare("Mr.");
    mstring newName = name.MReplace(ref oldS, "");
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring MReplace(string oldString, ref mstring newString)`

Replace all occurances of old string with the new mstring.  This mstring is changed and returned.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newS = mstring.Prepare("");
    mstring newName = name.MReplace("Mr.", ref newS);
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring MReplace(string oldString, string newString)`

Replace all occurances of old string with the new string.  This mstring is changed and returned.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newName = name.MReplace("Mr.", "");
    output.Add(newName, mstring.Prepare());
} 
```

---




## `ReplaceM` ##
`public mstring ReplaceM(char oldChar, char newChar)`

Replace all occurances of old char with the new char.  This mstring is changed and returned.
#### Remarks ####
This is equivalent to MReplace.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newName = name.ReplaceM('.', '-');
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring ReplaceM(ref mstring oldString, string newString)`

Replace all occurances of old mstring with the new string.  This mstring is changed and returned.
#### Remarks ####
This is equivalent to MReplace.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring oldS = mstring.Prepare("Mr.");
    mstring newName = name.ReplaceM(ref oldS, "");
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring ReplaceM(string oldString, ref mstring newString)`

Replace all occurances of old string with the new mstring.  This mstring is changed and returned.
#### Remarks ####
This is equivalent to MReplace.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newS = mstring.Prepare("");
    mstring newName = name.ReplaceM("Mr.", ref newS);
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring ReplaceM(string oldString, string newString)`

Replace all occurances of old string with the new string.  This mstring is changed and returned.
#### Remarks ####
This is equivalent to MReplace.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring newName = name.ReplaceM("Mr.", "");
    output.Add(newName, mstring.Prepare());
} 
```

---




`public mstring ReplaceM(ref mstring oldString, ref mstring newString)`

Replace all occurances of old mstring with the new mstring.  This mstring is changed and returned.
#### Remarks ####
This is equivalent to MReplace.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    mstring oldS = mstring.Prepare("Mr.");
    mstring newS = mstring.Prepare("");
    mstring newName = name.ReplaceM(ref oldS, ref newS);
    output.Add(newName, mstring.Prepare());
} 
```

---




# Static Methods of mstringarray #



## `Prepare` ##
`public static mstringarray Prepare()`

Returns an empty mstringarray with Length = 0.

#### Example ####
```
mstringarray arr2 = mstringarray.Prepare();

if(id == 54)
{
    arr2 = mstringarray.Prepare(10);
}

if(id == 778)
{
    arr2 = mstringarray.Prepare(19);
} 
```

---




`public static mstringarray Prepare(int length)`

Returns a mstringarray with the specified length.

#### Example ####
```
mstringarray arr2 = mstringarray.Prepare();

if(id == 54)
{
    arr2 = mstringarray.Prepare(10);
}

if(id == 778)
{
    arr2 = mstringarray.Prepare(19);
} 
```

---




# Properties and Indexer of `mstringarray` #



## `Length` ##
`public int Length{get;`}

Returns the length of this mstringarray.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstringarray arr = mstringarray.Prepare(10);

    for (int i = 0; i < arr.Length; i++)
    {
        arr[i] = mstring.Prepare(i);
    }

    mstring ms = mstring.Prepare();

    for (int i = 0; i < arr.Length; i++)
    {
        ms = ms.MAppend(arr[i]);
    }

    output.Add(ms, mstring.Prepare());
} 
```

---




## `Indexer` ##
`public mstring this[int i]`

Returns the i-th mstring in the array.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstringarray arr = mstringarray.Prepare(10);

    for (int i = 0; i < arr.Length; i++)
    {
        arr[i] = mstring.Prepare(i);
    }

    mstring ms = mstring.Prepare();

    for (int i = 0; i < arr.Length; i++)
    {
        ms = ms.MAppend(arr[i]);
    }

    output.Add(ms, mstring.Prepare());
} 
```

---




# Static methods of `recordset` #



## `Prepare` ##
`public static recordset Prepare()`

Initializes and returns a new recordset.

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




`public static recordset Prepare(ByteSlice b)`

Initializes and returns a new recordset using the buffer from ByteSlice.

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




## `PrepareRow` ##
`public static recordset PrepareRow(ByteSlice b)`

Initializes and returns a new recordset using the ByteSlice row.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    recordset rline = recordset.PrepareRow(line);
    int uid = rline.GetInt();
    mstring uname = rline.GetString();
    double grade = rline.GetDouble();

    recordset rkey = recordset.Prepare();
    rkey.PutInt(uid);

    recordset rvalue = recordset.Prepare();
    rvalue.PutString(uname);
    rvalue.PutDouble(grade);

    output.Add(rkey, rvalue);
} 
```

---




# Non-static methods of `recordset` #



## `PutInt32` ##
`public void PutInt32(Int32 x)`

Put a Int32 x into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

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




## `PutInt` ##
`public void PutInt(Int32 x)`

Put a Int32 x into the recordset.
#### Remarks ####
This is equivalent to PutInt32(Int32 x).

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

    rKey.PutInt(i1);
    rKey.PutInt(i2);

    rValue.PutInt64(i3);
    rValue.PutString(s);
    rValue.PutInt(i4);

    output.Add(rKey, rValue);
} 
```

---




## `PutUInt32` ##
`public void PutUInt32(UInt32 x)`

Put a UInt32 x into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    int i1 = ms.NextItemToInt32(',');
    int i2 = ms.NextItemToInt32(',');
    Int64 i3 = ms.NextItemToInt64(',');
    mstring s = ms.NextItemToString(',');
    UInt32 i4 = ms.NextItemToUInt32(',');

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString(s);
    rValue.PutUInt32(i4);

    output.Add(rKey, rValue);
} 
```

---




## `PutUInt` ##
`public void PutUInt(UInt32 x)`

Put a UInt32 x into the recordset.
#### Remarks ####
This is equivalent to PutUInt32.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    int i1 = ms.NextItemToInt32(',');
    int i2 = ms.NextItemToInt32(',');
    Int64 i3 = ms.NextItemToInt64(',');
    mstring s = ms.NextItemToString(',');
    UInt32 i4 = ms.NextItemToUInt32(',');

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString(s);
    rValue.PutUInt(i4);

    output.Add(rKey, rValue);
} 
```

---




## `PutInt64` ##
`public void PutInt64(Int64 x)`

Put a Int64 x into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

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




## `PutLong` ##
`public void PutLong(Int64 x)`

Put a Int64 x into the recordset.
#### Remarks ####
This is equivalent to PutInt64(Int64 x).

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

    rValue.PutLong(i3);
    rValue.PutString(s);
    rValue.PutInt32(i4);

    output.Add(rKey, rValue);
} 
```

---




## `PutUInt64` ##
`public void PutUInt64(UInt64 x)`

Put a UInt64 x into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    int i1 = ms.NextItemToInt32(',');
    int i2 = ms.NextItemToInt32(',');
    Int64 i3 = ms.NextItemToInt64(',');
    mstring s = ms.NextItemToString(',');
    UInt64 i4 = ms.NextItemToUInt64(',');

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString(s);
    rValue.PutUInt64(i4);

    output.Add(rKey, rValue);
} 
```

---




## `PutULong` ##
`public void PutULong(UInt64 x)`

Put a UInt64 x into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    int i1 = ms.NextItemToInt32(',');
    int i2 = ms.NextItemToInt32(',');
    Int64 i3 = ms.NextItemToInt64(',');
    mstring s = ms.NextItemToString(',');
    UInt64 i4 = ms.NextItemToUInt64(',');

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString(s);
    rValue.PutULong(i4);

    output.Add(rKey, rValue);
} 
```

---




## `PutDouble` ##
`public void PutDouble(double x)`

Put a double x into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring ms = mstring.Prepare(line);

    double x = ms.CsvNextItemToDouble();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    mstring s = ms.CsvNextItemToString();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutDouble(x);
    rKey.PutInt32(i2);

    rValue.PutLong(i3);
    rValue.PutString(s);
    rValue.PutInt32(i4);

    output.Add(rKey, rValue);
} 
```

---




## `PutString` ##
`public void PutString(mstring s)`

Put a mstring s into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

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




`public void PutString(string s)`

Put a string s into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,i
    //61,92,383,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString("--");
    rValue.PutInt32(i4);

    output.Add(rKey, rValue);
} 
```

---




## `PutChar` ##
`public void PutChar(char c)`

Put a char c into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,i
    //61,92,383,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString('-');
    rValue.PutInt32(i4);

    output.Add(rKey, rValue);
} 
```

---




## `PutBool` ##
`public void PutBool(bool o)`

Put a boolean o into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,i
    //61,92,383,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);

    if (i3 > 80)
    {
        rValue.PutBool(true);
    }
    else
    {
        rValue.PutBool(false);
    }

    rValue.PutInt32(i4);

    output.Add(rKey, rValue);
} 
```

---




## `PutDateTime` ##
`public void PutDateTime(DateTime x)`

Put the DateTime item x into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    DateTime dt = DateTime.Parse(sLine);
    recordset key = recordset.Prepare();
    key.PutDateTime(dt);
    output.Add(key, recordset.Prepare());
} 
```

---




## `PutInt16` ##
`public void PutInt16(Int16 x)`

Put a Int16 x into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,i
    //61,92,383,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutInt32(i4);

    Int16 m = (Int16)(i1 % 10);
    rValue.PutInt16(m);

    output.Add(rKey, rValue);
} 
```

---




## `PutShort` ##
`public void PutShort(Int16 x)`

Put a Int16 x into the recordset.
#### Remarks ####
This is equivalent to PutInt16.  The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,i
    //61,92,383,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutInt32(i4);

    Int16 m = (Int16)(i1 % 10);
    rValue.PutShort(m);

    output.Add(rKey, rValue);
} 
```

---




## `PutUInt16` ##
`public void PutUInt16(UInt16 x)`

Put a UInt16 x into the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,i
    //61,92,383,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutInt32(i4);

    Int16 m = (Int16)(i1 % 10);
    rValue.PutUInt16(m);

    output.Add(rKey, rValue);
} 
```

---




## `PutUShort` ##
`public void PutUShort(UInt16 x)`

Put a UInt16 x into the recordset.
#### Remarks ####
This is equivalent to PutUInt16.  The order of putting and getting items from the recordset must be the same.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,i
    //61,92,383,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutInt32(i4);

    Int16 m = (Int16)(i1 % 10);
    rValue.PutUShort(m);

    output.Add(rKey, rValue);
} 
```

---




## `PutBytes` ##
`public void PutBytes(IList<byte> bytes, int byteIndex, int byteCount)`

Put bytes into the recordset, starting at byteIndex, with byteCount number of bytes to put.

#### Example ####
```
byte[] buf = new byte[4];
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    int x = sLine.NextItemToInt(',');
    Entry.ToBytes(x, buf, 0);
    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutBytes(buf, 0, 2);

    output.Add(rKey, rValue);
} 
```

---




## `GetInt32` ##
`public Int32 GetInt32()`

Get the next Int32 from the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

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




## `GetInt` ##
`public Int32 GetInt()`

Get the next Int32 from the recordset.
#### Remarks ####
This is equivalent to GetInt32().

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    int i1 = rKey.GetInt();
    int i2 = rKey.GetInt();

    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        Int64 i3 = rValue.GetInt64();
        mstring s = rValue.GetString();
        int i4 = rValue.GetInt();
        mstring delimiter = mstring.Prepare(",");

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




## `GetUInt32` ##
`public UInt32 GetUInt32()`

Get the next UInt32 from the recordset.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        Int64 i3 = rValue.GetInt64();
        mstring s = rValue.GetString();
        UInt32 i4 = rValue.GetUInt32();
        mstring delimiter = mstring.Prepare(",");

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




## `GetUInt` ##
`public UInt32 GetUInt()`

Get the next UInt32 from the recordset.
#### Remarks ####
This is equivalent to GetUInt32.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        Int64 i3 = rValue.GetInt64();
        mstring s = rValue.GetString();
        UInt32 i4 = rValue.GetUInt();
        mstring delimiter = mstring.Prepare(",");

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




## `GetInt64` ##
`public Int64 GetInt64()`

Get the next Int64 from the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

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




## `GetLong` ##
`public Int64 GetLong()`

Get the next Int64 from the recordset.
#### Remarks ####
This is equivalent to GetInt64().

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

        Int64 i3 = rValue.GetLong();
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




## `GetUInt64` ##
`public UInt64 GetUInt64()`

Get the next UInt64 from the recordset.

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

        UInt64 i3 = rValue.GetUInt64();
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




## `GetULong` ##
`public UInt64 GetULong()`

Get the next UInt64 from the recordset.
#### Remarks ####
This is eqivalent to GetUInt64.

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

        UInt64 i3 = rValue.GetULong();
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




## `GetDouble` ##
`public Int64 GetDouble()`

Get the next double from the recordset.

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

        double i3 = rValue.GetDouble();
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




## `GetBytes` ##
`public void GetBytes(IList<byte> buffer, int offset, int byteCount)`

Get the next byteCount number of bytes from the recordset, and put into buffer, starting at offset.

#### Example ####
```
byte[] buf = new byte[10];
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    int i1 = rKey.GetInt32();
    int i2 = rKey.GetInt32();

    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        rValue.GetBytes(buf, 0, 10);
    }

    //...
} 
```

---




## `GetDateTime` ##
`public DateTime GetDateTime()`

Get the next DateTime item from the recordset.

#### Example ####
```
public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
{
    recordset rkey = recordset.Prepare(key);
    DateTime dt = rkey.GetDateTime();
    output.Add(mstring.Prepare(dt.ToString("yyyy-MM-dd hh:mm:ss")));
} 
```

---




## `GetString` ##
`public mstring GetString()`

Get the next mstring from the recordset.
#### Remarks ####
The order of putting and getting items from the recordset must be the same.

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




## `GetChar` ##
`public char GetChar()`

Get the next char from the recordset.

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

        double i3 = rValue.GetDouble();
        char s = rValue.GetChar();
        int i4 = rValue.GetInt32();
        mstring delimiter = mstring.Prepare(",");

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




## `GetBool` ##
`public bool GetBool()`

Get the next Boolean from the recordset.

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

        double i3 = rValue.GetDouble();
        char s = rValue.GetChar();
        int i4 = rValue.GetInt32();
        bool b = rValue.GetBool();

        if (b)
        {
            mstring delimiter = mstring.Prepare(",");
            ms.Consume(ref s);
            ms.Consume(ref delimiter);
        }
    }

    output.Add(ms);
} 
```

---




## `GetInt16` ##
`public Int16 GetInt16()`

Get the next Int16 from the recordset.

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

        Int16 i3 = rValue.GetInt16();
        char s = rValue.GetChar();
        int i4 = rValue.GetInt32();

        mstring delimiter = mstring.Prepare(",");
        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




## `GetShort` ##
`public Int16 GetShort()`

Get the next Int16 from the recordset.
#### Remarks ####
This is equivalent to GetInt16.

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

        Int16 i3 = rValue.GetShort();
        char s = rValue.GetChar();
        int i4 = rValue.GetInt32();

        mstring delimiter = mstring.Prepare(",");
        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




## `GetUInt16` ##
`public UInt16 GetUInt16()`

Get the next UInt16 from the recordset.

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

        UInt16 i3 = rValue.GetUInt16();
        char s = rValue.GetChar();
        int i4 = rValue.GetInt32();

        mstring delimiter = mstring.Prepare(",");
        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




## `GetUShort` ##
`public UInt16 GetUShort()`

Get the next UInt16 from the recordset.
#### Remarks ####
This is equivalent to GetUInt16.

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

        UInt16 i3 = rValue.GetUShort();
        char s = rValue.GetChar();
        int i4 = rValue.GetInt32();

        mstring delimiter = mstring.Prepare(",");
        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```

---




## `ToByteSlice` ##
`public ByteSlice ToByteSlice()`

Converts this recordset to a ByteSlice.  This recordset is changed after the conversion.
#### Remarks ####
It is adivsed not to re-use the recordset after it has been converted to a ByteSlice.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    int x = sLine.NextItemToInt(',');
    int y = sLine.NextItemToInt(',');
    mstring name = sLine.NextItemToString(',');

    recordset key = recordset.Prepare();
    key.PutInt(x);
    key.PutInt(y);

    recordset value = recordset.Prepare();
    value.PutString(name);

    ByteSlice bKey = key.ToByteSlice(10);
    ByteSlice bValue = value.ToByteSlice();

    output.Add(bKey, bValue);
} 
```

---




`public ByteSlice ToByteSlice(int size)`

Converts this recordset to a ByteSlice of length = size.   Padding will occur if the recordset if smaller than size.  If the recordset is bigger than size, it will throw an exception.  This recordset is changed after the conversion.
#### Remarks ####
It is adivsed not to re-use the recordset after it has been converted to a ByteSlice.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    int x = sLine.NextItemToInt(',');
    int y = sLine.NextItemToInt(',');
    mstring name = sLine.NextItemToString(',');

    recordset key = recordset.Prepare();
    key.PutInt(x);
    key.PutInt(y);

    recordset value = recordset.Prepare();
    value.PutString(name);

    ByteSlice bKey = key.ToByteSlice(10);
    ByteSlice bValue = value.ToByteSlice();

    output.Add(bKey, bValue);
} 
```