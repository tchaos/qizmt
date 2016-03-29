<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Character Encoding #

| **Language** | **Most Efficient Encoding to Use** |
|:-------------|:-----------------------------------|
|Chinese       |UTF-16                              |
|Hindustani    |UTF-16                              |
|Spanish       |UTF-8                               |
|English       |UTF-8                               |
|Arabic        |UTF-8                               |
|Portuguese    |UTF-8                               |
|Bengali       |UTF-16                              |
|Russian       |UTF-8                               |
|Japanese      |UTF-16                              |
|German        |UTF-8                               |
|Punjabi       |UTF-16                              |
|Telugu        |UTF-16                              |
|Marathi       |UTF-16                              |
|Vietnamese    |UTF-8                               |
|Korean        |UTF-16                              |
|Tamil         |UTF-16                              |
|French        |UTF-8                               |
|Italian       |UTF-8                               |

<br />

# Non-static methods of class `ByteSlice` #


## `ToString` ##

`public override string ToString()`

Converts this buffer to string using UTF-8 encoding.

#### Example ####

```
/*
This code uses this sample input:

K38000,Gary Doe
I56463,Joes Smith
A14546,John Smith
C94854,Mary Doe
Y139409,Sam Smith
E09809,John Doe
*/
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), ByteSlice.Prepare(parts[1]));
} 
```

---



## `ToBytes` ##
`public byte[] ToBytes()`

Returns a copy of this `ByteSlice`’s buffer.
#### Remarks ####
This method allocates a new `byte[]` each time when it is called.  Consider the more efficient methods `CopyTo` or `AppendTo` when necessary.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    byte[] myBuffer = key.ToBytes();

    //Examine the first byte of key.
    if (myBuffer[0] == 0x0)
    {
        string sKey = UnpadKey(key).ToString();
        output.Add(ByteSlice.Prepare("key=" + sKey));
    }
    else
    {
        //Do something else...
    }
} 
```

---



## `CopyTo` ##
`public void CopyTo(byte[] myBuffer)`

Copies this buffer into myBuffer.
#### Remarks ####
This is a more efficient version of the `ToBytes()` method.  A `byte[]` is passed in to retrieve the buffer, rather than allocating a brand new

byte array every time.

#### Example ####
In this example, a byte array myBuffer is allocated outside of the Reduce method.   This byte array will last for the lifetime of the slave.

Slaves are single threaded.

The Qizmt\_KeyLength constant is used here during the allocation of the byte array.  This ensures that the byte array is large enough when the key

buffer is copied into it.

```
//Allocate myBuffer outside of the Reduce method.
byte[] myBuffer = new Byte[Qizmt_KeyLength];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(myBuffer);

    //Examine the first byte of key.
    //key[0] works also.
    if (myBuffer[0] == 0x0)
    {
        string sKey = UnpadKey(key).ToString();
        output.Add(ByteSlice.Prepare("key=" + sKey));
    }
    else
    {
        //Do something else...
    }
} 
```

---




`public void CopyTo(byte[] myBuffer, int myBufferOffset)`

Copies this buffer into myBuffer starting at myBufferOffset.
#### Remarks ####
This is a more efficient version of the `ToBytes()` method.  A `byte[]` is passed in to retrieve the buffer, rather than allocating a brand new

byte array every time.

#### Example ####
```
//Allocate myBuffer outside of the Reduce method.
byte[] myBuffer = new Byte[16];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(myBuffer, 0);

    //Examine the first byte of key.
    if (myBuffer[0] == 0x0)
    {
        string sKey = UnpadKey(key).ToString();
        output.Add(ByteSlice.Prepare("key=" + sKey));
    }
    else
    {
        //Do something else...
    }
} 
```

---





## `AppendTo` ##
`public void AppendTo(List<byte> list)`

Appends this `ByteSlice`’s buffer to list.
#### Remarks ####
`List<byte>` is usually the easiest way to work with binary.

#### Example ####
```
List<byte> myBuffer = new List<byte>();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    myBuffer.Clear();
    key.AppendTo(myBuffer);

    if (myBuffer[0] == 0x61)
    {
        string sKey = UnpadKey(key).ToString();
        output.Add(ByteSlice.Prepare("key=" + sKey));
    }
    else
    {
        //Do something else.
    }
} 
```

---




# Static Methods of `ByteSlice` #



## `Prepare` ##
`public static ByteSlice Prepare(string x)`

Converts the string x into buffer using UTF-8 encoding.  A new `ByteSlice` instance is then created using this buffer.

#### Example ####
```
/*
This code uses this sample input:

K38000,Gary Doe
I56463,Joes Smith
A14546,John Smith
C94854,Mary Doe
Y139409,Sam Smith
E09809,John Doe
*/
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    output.Add(ByteSlice.Prepare(parts[0].PadRight(16, '\0')), ByteSlice.Prepare(parts[1]));
} 
```

---




`public static ByteSlice Prepare(StringBuilder x)`

Converts the StringBuilder instance to string, which is then converted to buffer using UTF-8 encoding.  A new `ByteSlice` instance is then created

using this buffer.

#### Example ####
```
StringBuilder sb = new StringBuilder();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    sb.Remove(0, sb.Length);

    for (int i = 0; i < values.Length; i++)
    {
        sb.Append(values[i].Value.ToString());
        sb.Append("|");
    }

    output.Add(ByteSlice.Prepare("Values:"));
    output.Add(ByteSlice.Prepare(sb));
} 
```

---




`public static ByteSlice Prepare(IList<byte> data)`

Creates a new `ByteSlice` instance using data.

#### Example 1 ####
```
byte[] valueBuffer = new byte[20];
byte[] keyBuffer = new byte[2];

public virtual void Map(ByteSlice line, MapOutput output)
{
    for (int i = 0; i < valueBuffer.Length; i++)
    {
        valueBuffer[0] = 0x0;
    }

    line.CopyTo(valueBuffer);

    keyBuffer[0] = valueBuffer[0];
    keyBuffer[1] = valueBuffer[1];

    output.Add(ByteSlice.Prepare(keyBuffer), ByteSlice.Prepare(valueBuffer));
} 
```



#### Example 2 ####
In this example, a `List<byte>` instance is passed into the `AppendTo` method to get the bytes of the line.
```
byte[] keyBuffer = new byte[2];

public virtual void Map(ByteSlice line, MapOutput output)
{
    valueBuffer.Clear();
    line.AppendTo(valueBuffer);

    keyBuffer[0] = valueBuffer[0];
    keyBuffer[1] = valueBuffer[1];

    output.Add(ByteSlice.Prepare(keyBuffer), ByteSlice.Prepare(valueBuffer));
}
```

---




`public static ByteSlice Prepare(IList<byte> data, int offset, int length)`

Creates a new `ByteSlice` instance using data, offset, and length.

#### Example 1 ####
```
byte[] myBuffer = new byte[10];

public virtual void Map(ByteSlice line, MapOutput output)
{
    for (int i = 0; i < myBuffer.Length; i++)
    {
        myBuffer[0] = 0x0;
    }

    line.CopyTo(myBuffer);

    //Using the first 4 bytes as key.
    output.Add(ByteSlice.Prepare(myBuffer, 0, 4), ByteSlice.Prepare("1"));
} 
```



#### Example 2 ####
```
List<byte> myBuffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    myBuffer.Clear();
    line.CopyTo(myBuffer);

    //Using the first 4 bytes as key.
    output.Add(ByteSlice.Prepare(myBuffer, 0, 4), ByteSlice.Prepare("1"));
}
```

---




`public static ByteSlice Prepare(ByteSlice data, int offset, int length)`

Creates a new `ByteSlice` instance using data, offset, and length.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //Using the first 4 bytes as key.
    output.Add(ByteSlice.Prepare(line, 0, 4), ByteSlice.Prepare("1"));
} 
```

---




`public static ByteSlice Prepare()`

Creates a new ByteSlice instance with empty buffer.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //Use the first 4 bytes as key and empty ByteSlice as key.
    output.Add(ByteSlice.Prepare(line, 0, 4), ByteSlice.Prepare());
} 
```

---




## `PreparePaddedStringAscii` ##
`public static ByteSlice PreparePaddedStringAscii(string x, int size)`

Converts the string into buffer using ASCII encoding.  If the length of buffer is larger than size, the buffer is truncated.  If the length of

buffer is smaller than size, the buffer is padded with Ø bytes.  A new ByteSlice instance is then created using this buffer.
#### Remarks ####
To recover the string, the buffer must be unpadded first using the `UnpadKey` method.   Please see example.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], 16), ByteSlice.Prepare(parts[1]));
}

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += values[i].Value.ToString();
    }
    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
} 
```

---




## `PreparePaddedStringUTF8` ##
`public static ByteSlice PreparePaddedStringUTF8(string x, int size)`

Converts the string into buffer using UTF-8 encoding.  If the length of buffer is larger than size, the buffer is truncated at character breaks.

If the length of buffer is smaller than size, the buffer is padded with Ø bytes.  A new `ByteSlice` instance is then created using this buffer.
#### Remarks ####
To recover the string, the buffer must be unpadded first using the UnpadKey method.   Please see example.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    output.Add(ByteSlice.PreparePaddedStringUTF8(parts[0], 16), ByteSlice.Prepare(parts[1]));
}

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += values[i].Value.ToString();
    }
    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
} 
```

---




# Static Methods of Entry #



## `ToBytes` ##
`public static byte[] ToBytes(Int32 x)`

Converts int x to bytes in big endian byte ordering.
#### Remarks ####
A new `byte[]` is allocated each time this method is called.  Consider a more efficient overload of this method in which a `byte[]` is passed in

as a parameter, or consider the `ToBytesAppend` method.

#### Example ####
```
//Map code
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();	
    byte[] buffer = Entry.ToBytes(Convert.ToInt32(sLine));
    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
}


//Reducer code
byte[] buffer = new byte[4];
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);
    int num = Entry.BytesToInt(buffer);
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




`public static void ToBytes(Int32 x, byte[] resultbuf, int bufoffset)`

Converts int x to bytes in big endian byte ordering with offset.
#### Remarks ####
Please see example 2 when sorting on a mixture of negative and positive integers.

#### Example 1 ####
This example sorts positive integers.

```
byte[] myBuffer = new byte[4];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    Entry.ToBytes(Convert.ToInt32(sLine), myBuffer, 0);
    output.Add(ByteSlice.Prepare(myBuffer), ByteSlice.Prepare());
} 
```

#### Example 2 ####
This example sorts a mixture of negative and positive integers.

```
//Map code
byte[] buffer = new byte[4];
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();

    UInt32 i = Entry.ToUInt32(Convert.ToInt32(sLine));
    Entry.UInt32ToBytes(i, buffer, 0);

    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
}

//Reducer code

byte[] buffer = new byte[4];
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);

    int num = Entry.ToInt32(Entry.BytesToUInt32(buffer));

    output.Add(ByteSlice.Prepare(num.ToString()));
}
```

---




## `ToBytesAppend` ##
`public static void ToBytesAppend(Int32 x, List<byte> list)`

Converts int x to bytes in big endian byte ordering and appends to list.

#### Example ####
```
List<byte> myBuffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    myBuffer.Clear();
    Entry.ToBytesAppend(Convert.ToInt32(sLine), myBuffer);
    output.Add(ByteSlice.Prepare(myBuffer), ByteSlice.Prepare());
} 
```

---




## `ToUInt32` ##
`public static UInt32 ToUInt32(Int32 x)`

Converts int x to UInt32.

#### Example ####
This example sorts a mixture of negative and positive integers.

```
//Map code
byte[] buffer = new byte[4];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();

    UInt32 i = Entry.ToUInt32(Convert.ToInt32(sLine));
    Entry.UInt32ToBytes(i, buffer, 0);

    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
}

//Reducer code
byte[] buffer = new byte[4];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);

    int num = Entry.ToInt32(Entry.BytesToUInt32(buffer));

    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




## `ToInt32` ##
`public static Int32 ToInt32(UInt32 x)`

Converts UInt32 to int.

#### Example ####
This example sorts a mixture of negative and positive integers.

```
//Map code
byte[] buffer = new byte[4];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();

    UInt32 i = Entry.ToUInt32(Convert.ToInt32(sLine));
    Entry.UInt32ToBytes(i, buffer, 0);

    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
}

//Reducer code
byte[] buffer = new byte[4];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);

    int num = Entry.ToInt32(Entry.BytesToUInt32(buffer));

    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




## `UInt32ToBytes` ##
`public static byte[] UInt32ToBytes(UInt32 x)`

Converts UInt32 x to bytes in big endian byte ordering.   A new byte array is allocated each time this method is called.  Consider a more

efficient overload of this method or `UInt32ToBytesAppend`.

#### Example ####
This example sorts a mixture of negative and positive integers.

```
//Map code
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();

    UInt32 i = Entry.ToUInt32(Convert.ToInt32(sLine));
    byte[] buffer = Entry.UInt32ToBytes(i);

    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
}

//Reducer code
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();

    UInt32 i = Entry.ToUInt32(Convert.ToInt32(sLine));
    byte[] buffer = Entry.UInt32ToBytes(i);

    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
} 
```

---




`public static void UInt32ToBytes(UInt32 x, byte[] buffer, int offset)`

Converts UInt32 x to bytes in big endian byte ordering.

#### Example ####
This example sorts a mixture of negative and positive integers.

```
//Map code
byte[] buffer = new byte[4];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();

    UInt32 i = Entry.ToUInt32(Convert.ToInt32(sLine));
    Entry.UInt32ToBytes(i, buffer, 0);

    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
}

//Reducer code
byte[] buffer = new byte[4];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);

    int num = Entry.ToInt32(Entry.BytesToUInt32(buffer));

    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




## `UInt32ToBytesAppend` ##
`public static void UInt32ToBytesAppend(UInt32 x, List<byte> buffer)`

Converts UInt32 x to bytes in big endian byte ordering and appends to buffer.

#### Example ####
This example sorts a mixture of negative and positive integers.

```
//Map code
List<byte> buffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();

    UInt32 i = Entry.ToUInt32(Convert.ToInt32(sLine));

    buffer.Clear();
    Entry.UInt32ToBytesAppend(i, buffer);

    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
}

//Reducer code
byte[] buffer = new byte[4];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);

    int num = Entry.ToInt32(Entry.BytesToUInt32(buffer));

    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




## `UInt16ToBytes` ##
`public static byte[] UInt16ToBytes(UInt16 x)`

Converts UInt16  x to bytes in big endian byte ordering.
#### Remarks ####
A byte array is allocated each time this method is called.  Consider a more efficient overload of this method.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    UInt16 i = Convert.ToUInt16(sLine);
    byte[] buffer = Entry.UInt16ToBytes(i);
    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare(""));
} 
```

---




`public static void UInt16ToBytes(UInt16 x, byte[] resultbuf, int bufoffset)`

Converts UInt16  x to bytes in big endian byte ordering.

#### Example ####
```
byte[] buffer = new byte[2];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    UInt16 i = Convert.ToUInt16(sLine);
    Entry.UInt16ToBytes(i, buffer, 0);
    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare(""));
} 
```

---




## `Int16ToBytes` ##
`public static byte[] Int16ToBytes(Int16 x)`

Converts Int16 x to bytes in big endian byte ordering.
#### Remarks ####
A new `byte[]` is allocated each time this method is called.  Consider a more efficient overload of this method in which a `byte[]` is passed in

as a parameter.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    byte[] myBuffer = Entry.Int16ToBytes(Convert.ToInt16(sLine));
    output.Add(ByteSlice.Prepare(myBuffer), ByteSlice.Prepare());
} 
```

---




`public static void Int16ToBytes(Int16 x, byte[] resultBuffer, int offset)`

Converts Int16 x to bytes in big endian byte ordering with offset.

#### Example ####
```
byte[] myBuffer = new byte[2];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    Entry.Int16ToBytes(Convert.ToInt16(sLine), myBuffer, 0);
    output.Add(ByteSlice.Prepare(myBuffer), ByteSlice.Prepare());
} 
```

---




## `ToUInt16` ##
`public static UInt16 ToUInt16(Int16 x)`

Converts Int16 x to UInt16.

#### Example ####
This example sorts a mixture of positive and negative integers of type Int16.

```
//Map code
byte[] buffer = new byte[2];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    UInt16 i = Entry.ToUInt16(Convert.ToInt16(sLine));
    Entry.UInt16ToBytes(i, buffer, 0);
    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare(""));
}

//Reducer code
byte[] buffer = new byte[2];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);
    Int16 num = Entry.ToInt16(Entry.BytesToUInt16(buffer));
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




## `ToInt16` ##
`public static Int16 ToInt16(UInt16 x)`

Converts UInt16 x to Int16.

#### Example ####
This example sorts a mixture of positive and negative integers of type Int16.

```
//Map code
byte[] buffer = new byte[2];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    UInt16 i = Entry.ToUInt16(Convert.ToInt16(sLine));
    Entry.UInt16ToBytes(i, buffer, 0);
    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare(""));
}

//Reducer code
byte[] buffer = new byte[2];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);
    Int16 num = Entry.ToInt16(Entry.BytesToUInt16(buffer));
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




## `LongToBytes` ##
`public static void LongToBytes(long x, byte[] resultBuffer, int offset)`

Converts long x to bytes in big endian byte ordering at offset.

#### Example ####
```
byte[] myBuffer = new byte[8];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    Entry.LongToBytes(Convert.ToInt64(sLine), myBuffer, 0);
    output.Add(ByteSlice.Prepare(myBuffer), ByteSlice.Prepare());
} 
```

---




## `ToBytesAppend64` ##
`public static void ToBytesAppend64(Int64 x, List<byte> list)`

Converts int x to bytes in big endian byte ordering and appends to list.

#### Example ####
```
List<byte> myBuffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    myBuffer.Clear();
    Entry.ToBytesAppend64(Convert.ToInt64(sLine), myBuffer);
    output.Add(ByteSlice.Prepare(myBuffer), ByteSlice.Prepare());
} 
```

---




## `BytesToInt` ##
`public static Int32 BytesToInt(IList<byte> x)`

Converts bytes, which are in big endian byte ordering, to int.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    int num = Entry.BytesToInt(key.ToBytes());
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




`public static Int32 BytesToInt(IList<byte> x, int offset)`

Converts bytes, which is in big endian byte ordering, starting at offset to int.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    int num = Entry.BytesToInt(key.ToBytes(), 0);
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




## `BytesToUInt32` ##
`public static UInt32 BytesToUInt32(IList<byte> x)`

Converts bytes, which are in big endian byte ordering, to UInt32.

#### Example ####
This example sorts a mixture of negative and positive integers.

```
//Map code
byte[] buffer = new byte[4];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();

    UInt32 i = Entry.ToUInt32(Convert.ToInt32(sLine));
    Entry.UInt32ToBytes(i, buffer, 0);

    output.Add(ByteSlice.Prepare(buffer), ByteSlice.Prepare());
}

//Reducer code
byte[] buffer = new byte[4];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);

    int num = Entry.ToInt32(Entry.BytesToUInt32(buffer));

    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




## `BytesToUInt16` ##
`public static UInt16 BytesToUInt16(IList<byte> x)`

Converts the bytes, which are in big endian byte ordering, to UInt16.

#### Example ####
```
byte[] buffer = new byte[2];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);

    UInt16 num = Entry.BytesToUInt16(buffer);

    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




`public static UInt16 BytesToUInt16(IList<byte> x, int offset)`

Converts the bytes, which are in big endian byte ordering, to UInt16, starting at offset.

#### Example ####
```
byte[] buffer = new byte[2];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    key.CopyTo(buffer);

    UInt16 num = Entry.BytesToUInt16(buffer, 0);

    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




## `BytesToLong` ##
`public static Int64 BytesToLong(IList<byte> x)`

Converts bytes, which are in big endian byte ordering, to long.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    Int64 num = Entry.BytesToLong(key.ToBytes());
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




`public static long BytesToLong(IList<byte> x, int offset)`

Converts bytes, which are in big endian byte ordering, starting at offset to long.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    Int64 num = Entry.BytesToLong(key.ToBytes(), 0);
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




## `BytesToInt16` ##
`public static Int16 BytesToInt16(IList<byte> x)`

Converts bytes, which are in big endian byte ordering, to int.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    Int16 num = Entry.BytesToInt16(key.ToBytes());
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




`public static Int16 BytesToInt16(IList<byte> x, int offset)`

Converts bytes, which are in big endian byte ordering, starting at offset to int.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    Int16 num = Entry.BytesToInt16(key.ToBytes(), 0);
    output.Add(ByteSlice.Prepare(num.ToString()));
} 
```

---




## `AsciiToBytesAppend` ##
`public static void AsciiToBytesAppend(StringBuilder x, List<byte> list)`

Converts string to bytes using ASCII encoding and appends to list.

#### Example ####
```
StringBuilder sb = new StringBuilder();
List<byte> myBuffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');

    myBuffer.Clear();
    sb.Length = 0;
    sb.Append(parts[1]);

    for (int i = 0; i < sb.Length; i++)
    {
        //Code for manipulating the StringBuilder...                  
    }

    Entry.AsciiToBytesAppend(sb, myBuffer);
    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), ByteSlice.Prepare(myBuffer));
} 
```

---




`public static void AsciiToBytesAppend(string x, List<byte> list)`

Converts string to bytes using ASCII encoding and appends to list.

#### Example ####
```
List<byte> myBuffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    myBuffer.Clear();
    string value = parts[1];

    for (int i = 0; i < value.Length; i++)
    {
        //Code for manipulating the string...                  
    }

    Entry.AsciiToBytesAppend(value, myBuffer);
    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), ByteSlice.Prepare(myBuffer));
} 
```

---




## `AsciiToBytes` ##
`public static void AsciiToBytes(string x, byte[] buffer, int offset)`

Converts string to bytes using ASCII encoding and appends to buffer with offset.

#### Example ####
```
byte[] myBuffer = new byte[10];

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    string value = parts[1];
    for (int i = 0; i < value.Length; i++)
    {
        //Some code for manipulating the string...                  
    }

    Entry.AsciiToBytes(value, myBuffer, 0);
    output.Add(ByteSlice.Prepare(parts[0].PadRight(16, '\0')), ByteSlice.Prepare(myBuffer));
} 
```

---




## `BytesToAscii` ##
`public static string BytesToAscii(IList<byte> x)`

Converts bytes to string using ASCII encoding.
#### Remarks ####
A new string is allocated each time this method is called.  Consider the more efficient method `BytesToAsciiAppend`.

#### Example ####
```
byte[] myBuffer = new byte[10];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        values[i].Value.CopyTo(myBuffer);
        sValue = Entry.BytesToAscii(myBuffer);

        //Code for manipulating the string...

    }

    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
} 
```

---




`public static string BytesToAscii(IList<byte> x, int offset, int length)`

Converts bytes to string using ASCII encoding, starting from offset.  Length is the number of bytes to convert.  A string is allocated each time

this method is called.  Consider the more efficient method `BytesToAsciiAppend`.

#### Example ####
```
byte[] myBuffer = new byte[10];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        values[i].Value.CopyTo(myBuffer);
        sValue = Entry.BytesToAscii(myBuffer, 0, 5);

        //Code for manipulating the string...

    }

    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
} 
```

---




## `BytesToAsciiAppend` ##
`public static void BytesToAsciiAppend(IList<byte> x, StringBuilder sb)`

Converts bytes to string using ASCII encoding.

#### Example ####
```
byte[] myBuffer = new byte[10];
StringBuilder sb = new StringBuilder();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    output.Add(ByteSlice.Prepare("key=" + sKey));

    for (int i = 0; i < values.Length; i++)
    {
        values[i].Value.CopyTo(myBuffer);
        sb.Remove(0, sb.Length);
        Entry.BytesToAsciiAppend(myBuffer, sb);

        //Code for manipulating the string...     


        output.Add(ByteSlice.Prepare(sb));
    }
} 
```

---




`public static void BytesToAsciiAppend(IList<byte> x, StringBuilder buf, int offset, int length)`

Converts bytes to string using ASCII encoding, starting from offset.  Length is the number of bytes to convert.

#### Example ####
```
byte[] myBuffer = new byte[10];
StringBuilder sb = new StringBuilder();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    output.Add(ByteSlice.Prepare("key=" + sKey));

    for (int i = 0; i < values.Length; i++)
    {
        values[i].Value.CopyTo(myBuffer);
        sb.Remove(0, sb.Length);
        Entry.BytesToAsciiAppend(myBuffer, sb, 0, 10);

        //Code for manipulating the string...     


        output.Add(ByteSlice.Prepare(sb));
    }
} 
```

---




# Qizmt`_``*` Constants #



| **Name** | **Description** |
|:---------|:----------------|
| `const int Qizmt_ProcessID` | ID of current process in cluster, 0 to `Qizmt_ProcessCount` – 1.  |
| `const int Qizmt_ProcessCount` | Total number of processes for the job, usually equal to the number of cores or a prime near the number of cores. |
| `const string Qizmt_MachineHost` | Host name of the machine that the current mapper, reducer, local or remote is running on.  |
| `const string Qizmt_MachineIP` | IP address of the machine that the current mapper, reducer, local or remote is running on.  |
| `const string[] Qizmt_ExecArgs` | 	Command line arguments sent in with the Qizmt exec command  |
| `const int Qizmt_KeyLength` | IOSettings/ KeyLength  |
| `const string Qizmt_LogName` | 	Name of log file |


<br />


# Qizmt`_``*` Methods #


## `Qizmt_Log` ##
`public static void Qizmt_Log(string msg)`

Displays a message line to the console of the machine that executed.
#### Remarks ####
This is helpful for debugging purposes.  Logs are displayed after the job is completed, not real time.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    Qizmt_Log("key=" + sKey);
} 
```


<img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_Log.png' alt='Qizmt Log' />



---




# Methods of `MapOutput` #



## `Add` ##
`public void Add(ByteSlice key, ByteSlice value)`

Adds the key value pair to Map, with the offsets of key and value.

#### Example 1 ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    output.Add(ByteSlice.Prepare(parts[0].PadRight(16, '\0')), ByteSlice.Prepare(parts[1]));
} 
```



#### Example 2 ####
This example demonstrates what happens when changing the buffer after the ByteSlice has already been created, but before it is added to the

MapOutput.  You will see the change in the MapOutput.

```
//Map code
List<byte> buffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');

    buffer.Clear();
    Entry.AsciiToBytesAppend(parts[1], buffer);

    ByteSlice val = ByteSlice.Prepare(buffer);

    //Change one byte in the buffer.  This change will be carried to the MapOutput.
    buffer[0] = 0x0;

    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), val);
}

//Reducer code
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values[i].Value.ToString();
    }

    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
}
/*
Sample Input:

123,apple
234,lemon
444,berry

Sample Output:

key=123
values=, pple
key=234
values=, emon
key=444
values=, erry
*/
```


#### Example 3 ####
This example demonstrates what happens when changing the buffer after the ByteSlice has already been created and after it has been added to the

MapOutput.  You will NOT see the change in the MapOutput.

```
//Map code
List<byte> buffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');

    buffer.Clear();
    Entry.AsciiToBytesAppend(parts[1], buffer);

    ByteSlice val = ByteSlice.Prepare(buffer);

    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), val);

    //Then change one byte in the buffer.  This change will NOT be carried to the MapOutput.
    buffer[0] = 0x0;
}

//Reducer code
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values[i].Value.ToString();
    }

    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
}
/*
Sample Input:

123,apple
234,lemon
444,berry

Sample Output:

key=123
values=,apple
key=234
values=,lemon
key=444
values=,berry
*/
```

---




`public void Add(recordset key, recordset value)`

Adds a key value pair to map.
#### Remarks ####
When key value pair is added to Map using recordset, you must retrieve the key value pair using recordset in the Reduce method.

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




`public void Add(mstring key, mstring value)`

Adds a key value mstring pair to map.

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

    output.Add(key, name);
}

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    output.Add(sKey);

    mstring delimiter = mstring.Prepare(",");
    mstring sValues = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        mstring name = mstring.Prepare(values[i].Value);
        sValues = sValues.MAppend(name);
        sValues = sValues.MAppend(delimiter);
    }

    output.Add(sValues);
} 
```

---




`public void Add(mstring key, recordset value)`

Adds a key value pair to map.
#### Remarks ####
Since the value is added to Map using recordset, you must use recordset to retrieve the value back in the Reduce method.

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

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    output.Add(sKey);

    mstring delimiter = mstring.Prepare(",");
    mstring sValues = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rs = recordset.Prepare(values[i].Value);
        mstring name = rs.GetString();
        sValues = sValues.MAppend(name);
        sValues = sValues.MAppend(delimiter);
    }

    output.Add(sValues);
} 
```

---




# Methods of `ReduceOutput` (Equivalent to `RandomAccessOutput`) #


## `Add` ##
`public void Add(ByteSlice entry)`

Adds entry’s buffer to output file with a new line

#### Example 1 ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
        sValue += "," + values[i].Value.ToString();

    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
} 
```


#### Example 2 ####
This example demonstrates what happens when changing the buffer after the ByteSlice has already been created, but before it is added to the

RandomAccesOutput.  You will see the change in the RandomAccesOutput.

```
//Map code
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');

    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), ByteSlice.Prepare(parts[1]));

}

//Reducer code
byte[] buffer = new byte[Qizmt_KeyLength + 4];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string s = "key=" + UnpadKey(key).ToString();

    Entry.AsciiToBytes(s, buffer, 0);

    ByteSlice b = ByteSlice.Prepare(buffer);

    //Change one byte in the buffer.  This change will be carried to the output.
    buffer[0] = 0x0;

    output.Add(b);
}
/*
Sample Input:

123,apple
234,lemon
444,berry

Sample Output:

key=123            
key=234             
key=444 
*/
```


#### Example 3 ####
This example demonstrates what happens when changing the buffer after the ByteSlice has already been created and after it has been added to the

RandomAccessOutput.  You will NOT see the change in the RandomAccessOutput.

```
//Map code
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');

    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), ByteSlice.Prepare(parts[1]));

}

//Reducer code
byte[] buffer = new byte[Qizmt_KeyLength + 4];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string s = "key=" + UnpadKey(key).ToString();

    Entry.AsciiToBytes(s, buffer, 0);

    ByteSlice b = ByteSlice.Prepare(buffer);

    output.Add(b);

    //Change one byte in the buffer.  This change will NOT be carried to the output.
    buffer[0] = 0x0;

}
/*
Sample Input:

123,apple
234,lemon
444,berry

Sample Output:

key=123            
key=234             
key=444 
*/
```

---




`public void Add(mstring s)`

Writes the mstring to the output file.

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




## `WriteLine` ##
`public void WriteLine(mstring s)`

Writes the mstring to the output file.

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

    output.WriteLine(ms);
} 
```

---




## `Cache` ##
`public void Cache(ByteSlice key, ByteSlice value)`

Writes key and value to explicit cache.
#### Remarks ####
Once a key-value pair has been added to explicit cache, the next time mapReducer runs, the values will be passed back in.  Keys cannot be changed.

> Explicit cache overrides the automatic cache.  It speeds up subsequent runs.

#### Example ####
Specify cache configurations:

```
<Delta>
    <Name>MyMapReduce_Cache</Name>
    <DFSInput>dfs://data_input_*.txt</DFSInput>
</Delta>
<IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    <DFSInput>dfs://NOTHING*NOTHING</DFSInput>
    <DFSOutput>dfs://MyMapReduce_Output.txt</DFSOutput>
</IOSettings>
```


```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values[i].Value.ToString();
        output.Cache(key, values[i].Value);
    }

    output.Add(ByteSlice.Prepare("key=" + sKey), true);
    output.Add(ByteSlice.Prepare("values=" + sValue), true);
} 
```

---




`public void Cache(string key, string value)`

Writes key string and value string to explicit cache.
#### Remarks ####
Once a key-value pair has been added to explicit cache, the next time mapReducer runs, the values will be passed back in.  Keys cannot be changed.

> Explicit cache overrides the automatic cache.  It speeds up subsequent runs.

#### Example ####
Specify cache configurations:

```
<Delta>
    <Name>MyMapReduce_Cache</Name>
    <DFSInput>dfs://data_input_*.txt</DFSInput>
</Delta>
<IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    <DFSInput>dfs://NOTHING*NOTHING</DFSInput>
    <DFSOutput>dfs://MyMapReduce_Output.txt</DFSOutput>
</IOSettings>
```


```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    mstring sLine = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        mstring sValue = mstring.Prepare(values[i].Value);
        sLine.AppendM(sValue);
        sLine.AppendM(',');

        output.Cache("1", "10");
    }

    output.Add(sKey);
    output.Add(sLine);
}
```

---




`public void Cache(recordset key, recordset value)`

Writes key recordset and value recordset to explicit cache.
#### Remarks ####
Once a key-value pair has been added to explicit cache, the next time mapReducer runs, the values will be passed back in.  Keys cannot be changed.

> Explicit cache overrides the automatic cache.  It speeds up subsequent runs.

#### Example ####
Specify cache configurations:

```
<Delta>
    <Name>MyMapReduce_Cache</Name>
    <DFSInput>dfs://data_input_*.txt</DFSInput>
</Delta>
<IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    <DFSInput>dfs://NOTHING*NOTHING</DFSInput>
    <DFSOutput>dfs://MyMapReduce_Output.txt</DFSOutput>
</IOSettings>
```


```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(UnpadKey(key));

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        output.Cache(rKey, recordset.Prepare());
    }
} 
```

---




`public void Cache(mstring key, mstring value)`

Writes key mstring and value mstring to explicit cache.
#### Remarks ####
Once a key-value pair has been added to explicit cache, the next time mapReducer runs, the values will be passed back in.  Keys cannot be changed.

> Explicit cache overrides the automatic cache.  It speeds up subsequent runs.

#### Example ####
Specify cache configurations:

```
<Delta>
    <Name>MyMapReduce_Cache</Name>
    <DFSInput>dfs://data_input_*.txt</DFSInput>
</Delta>
<IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    <DFSInput>dfs://NOTHING*NOTHING</DFSInput>
    <DFSOutput>dfs://MyMapReduce_Output.txt</DFSOutput>
</IOSettings>
```


```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    mstring sLine = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        mstring sValue = mstring.Prepare(values[i].Value);
        sLine.AppendM(sValue);
        sLine.AppendM(',');

        output.Cache(sKey, mstring.Prepare());
    }

    output.Add(sKey);
    output.Add(sLine);
} 
```

---




`public void Cache(mstring key, recordset value)`

Writes key mstring and value recordset to explicit cache.
#### Remarks ####
Once a key-value pair has been added to explicit cache, the next time mapReducer runs, the values will be passed back in.  Keys cannot be changed.

> Explicit cache overrides the automatic cache.  It speeds up subsequent runs.

#### Example ####
Specify cache configurations:

```
<Delta>
    <Name>MyMapReduce_Cache</Name>
    <DFSInput>dfs://data_input_*.txt</DFSInput>
</Delta>
<IOSettings>
    <JobType>mapreduce</JobType>
    <KeyLength>100</KeyLength>
    <DFSInput>dfs://NOTHING*NOTHING</DFSInput>
    <DFSOutput>dfs://MyMapReduce_Output.txt</DFSOutput>
</IOSettings>
```


```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));

    for (int i = 0; i < values.Length; i++)
    {
        recordset rs = recordset.Prepare(values[i].Value);

        output.Cache(sKey, rs);
    }
} 
```

---




# Properties and Indexer of `ByteSliceList` (Equivalent to `RandomAccessEntries`) #


## `Length` ##
`public int Length{get;`}

Returns the count of entries that are passed into reducer.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values[i].Value.ToString();
    }

    output.Add(ByteSlice.Prepare("key=" + sKey), true);
    output.Add(ByteSlice.Prepare("values=" + sValue), true);
} 
```

---




## `Current` ##
`public ByteSlice Current{get;`}

Returns the current value ByteSlice.

#### Example ####
```
public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    mstring sValue = mstring.Prepare();

    while (values.MoveNext())
    {
        sValue.AppendM(',');
        sValue.AppendM(mstring.Prepare(values.Current()));
    }

    output.Add(sValue);
} 
```

---




## `Indexer` ##
`public ReduceEntry this[int i]`

Returns the i-th ReduceEntry.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values[i].Value.ToString();
    }

    output.Add(ByteSlice.Prepare("key=" + sKey), true);
    output.Add(ByteSlice.Prepare("values=" + sValue), true);
} 
```

---




## `Items` ##
`public RandomAccessEntriesItems Items`

Returns the value ByteSlices.

#### Example ####
```
public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values.Items[i].ToString();
    }

    output.Add(ByteSlice.Prepare("key=" + sKey), true);
    output.Add(ByteSlice.Prepare("values=" + sValue), true);
} 
```

---




# Methods of `ByteSliceList` #



## `MoveNext` ##
`public bool MoveNext()`

Moves the current position to the next value ByteSlice.  If there is no more value ByteSlice left, it returns false.

#### Example ####
```
public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    mstring sValue = mstring.Prepare();

    while (values.MoveNext())
    {
        sValue.AppendM(',');
        sValue.AppendM(mstring.Prepare(values.Current()));
    }

    output.Add(sValue);
} 
```

---




## `Reset` ##
`public void Reset()`

Moves the current position to the first value ByteSlice.

#### Example ####
```
public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    mstring sValue = mstring.Prepare();

    while (values.MoveNext())
    {
        sValue.AppendM(',');
        sValue.AppendM(mstring.Prepare(values.Current()));
    }

    values.Reset();

    while (values.MoveNext())
    {
        sValue.AppendM('-');
        sValue.AppendM(mstring.Prepare(values.Current()));
    }

    output.Add(sValue);
} 
```

---




# Properties of `ReduceEntry` #



## `Key` ##
`public ByteSlice Key`

Returns the Key of this entry.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    for (int i = 0; i < values.Length; i++)
    {
        ReduceEntry entry = values[i];
        output.Add(entry.Key);
        output.Add(entry.Value);
    }
} 
```

---




## `Value` ##
`public ByteSlice Value`

Returns the Value of this entry.

#### Example ####
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    for (int i = 0; i < values.Length; i++)
    {
        ReduceEntry entry = values[i];
        output.Add(entry.Key);
        output.Add(entry.Value);
    }
} 
```

---




# Methods of `ReduceEntry` #



## `CopyKey` ##
`public void CopyKey(byte[] buffer)`

Copies this key into buffer.

#### Example ####
```
byte[] myBuffer = new byte[Qizmt_KeyLength];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{

    ReduceEntry entry = values[0];

    //This is equivalent to:
    //key.CopyTo(myBuffer);
    entry.CopyKey(myBuffer);

    //Code to examine the first byte of key.
    if (myBuffer[0] == (byte)'a')
    {
        //Do something...

    }
    else
    {
        //Do something else...

    }
} 
```

---




`public void CopyKey(byte[] buffer, int bufferOffset)`

Copies this key into buffer starting at bufferOffset.

#### Example ####
```
byte[] myBuffer = new byte[10];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    ReduceEntry entry = values[0];

    //This is equivalent to:
    //key.CopyTo(myBuffer);
    entry.CopyKey(myBuffer, 0);

    //Code to examine the first byte of key.
    if (myBuffer[0] == (byte)'a')
    {
        //Do something...

    }
    else
    {
        //Do something else...

    }
} 
```

---




## `AppendKey` ##
`public void AppendKey(List<byte> list)`

Appends this key to list.

#### Example ####
```
List<byte> myBuffer = new List<byte>();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    ReduceEntry entry = values[0];

    myBuffer.Clear();

    //This is equivalent to:
    //key.AppendTo(myBuffer);
    entry.AppendKey(myBuffer);

    //Code to examine the first byte of key.
    if (myBuffer[0] == 0x61)
    {
        //Do something...
        output.Add(ByteSlice.Prepare("xxx"));
    }
    else
    {
        //Do something else...

    }
} 
```

---




## `CopyValue` ##
`public void CopyValue(byte[] buffer)`

Copies this value into buffer.

#### Example ####
```
byte[] myBuffer = new byte[10];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{

    for (int i = 0; i < values.Length; i++)
    {
        ReduceEntry entry = values[i];
        entry.CopyValue(myBuffer);

        if (myBuffer[0] == 0x61)
        {
            //Do something...
        }
        else
        {
            //Do something else...
        }
    }

} 
```

---




`public void CopyValue(byte[] buffer, int bufferOffset)`

Copies this value into buffer starting at bufferOffset.

#### Example ####
```
byte[] myBuffer = new byte[10];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{

    for (int i = 0; i < values.Length; i++)
    {
        ReduceEntry entry = values[i];
        entry.CopyValue(myBuffer, 0);

        if (myBuffer[0] == 0x61)
        {
            //Do something...
        }
        else
        {
            //Do something else...
        }
    }

} 
```

---




## `AppendValue` ##
`public void AppendValue(List<byte> list)`

Appends this value to a byte list.

#### Example ####
```
List<byte> myBuffer = new List<byte>();

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{

    for (int i = 0; i < values.Length; i++)
    {
        ReduceEntry entry = values[i];
        myBuffer.Clear();
        entry.AppendValue(myBuffer);

        if (myBuffer[0] == 0x61)
        {
            //Do something...
            output.Add(ByteSlice.Prepare("xxx"));
        }
        else
        {
            //Do something else...
        }
    }

} 
```