<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



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