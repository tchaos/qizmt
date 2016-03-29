<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `Consume` #
`public void Consume(ref mstring s)`

Appends mstring s and destroys s.

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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

### Example ###
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