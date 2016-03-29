<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `MAppend` #
`public mstring MAppend(mstring s)`

Appends mstring s.

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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




# `AppendM` #
`public mstring AppendM(mstring s)`

Appends mstring s.
### Remarks ###
This is equivalent to MAppend(mstring s).

### Example ###
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
### Remarks ###
This is equivalent to MAppend(char c).

### Example ###
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
### Remarks ###
This is equivalent to MAppend(double x).

### Example ###
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
### Remarks ###
This is equivalent to MAppend(Int16 x).

### Example ###
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
### Remarks ###
This is equivalent to MAppend(UInt16 x).

### Example ###
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
### Remarks ###
This is equivalent to MAppend(Int32 x).

### Example ###
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
### Remarks ###
This is equivalent to MAppend(UInt32 x).

### Example ###
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
### Remarks ###
This is equivalent to MAppend(Int64 x).

### Example ###
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
### Remarks ###
This is equivalent to MAppend(UInt64 x).

### Example ###
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