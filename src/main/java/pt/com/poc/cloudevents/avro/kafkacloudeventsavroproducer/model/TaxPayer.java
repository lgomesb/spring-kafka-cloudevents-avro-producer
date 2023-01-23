package pt.com.poc.cloudevents.avro.kafkacloudeventsavroproducer.model;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class TaxPayer extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -514314874589611510L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TaxPayer\",\"namespace\":\"com.irs.register.avro.taxpayer\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"Id of TaxPayer\"},{\"name\":\"name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"Name of TaxPayer\"},{\"name\":\"document\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"Document of TaxPayer\"},{\"name\":\"situation\",\"type\":\"boolean\",\"doc\":\"Legal situation of TaxPayer\",\"default\":false}],\"version\":\"2\"}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<TaxPayer> ENCODER =
      new BinaryMessageEncoder<TaxPayer>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<TaxPayer> DECODER =
      new BinaryMessageDecoder<TaxPayer>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<TaxPayer> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<TaxPayer> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<TaxPayer> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<TaxPayer>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this TaxPayer to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a TaxPayer from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a TaxPayer instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static TaxPayer fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  /** Id of TaxPayer */
   private java.lang.String id;
  /** Name of TaxPayer */
   private java.lang.String name;
  /** Document of TaxPayer */
   private java.lang.String document;
  /** Legal situation of TaxPayer */
   private boolean situation;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public TaxPayer() {}

  /**
   * All-args constructor.
   * @param id Id of TaxPayer
   * @param name Name of TaxPayer
   * @param document Document of TaxPayer
   * @param situation Legal situation of TaxPayer
   */
  public TaxPayer(java.lang.String id, java.lang.String name, java.lang.String document, java.lang.Boolean situation) {
    this.id = id;
    this.name = name;
    this.document = document;
    this.situation = situation;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return id;
    case 1: return name;
    case 2: return document;
    case 3: return situation;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: id = value$ != null ? value$.toString() : null; break;
    case 1: name = value$ != null ? value$.toString() : null; break;
    case 2: document = value$ != null ? value$.toString() : null; break;
    case 3: situation = (java.lang.Boolean)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'id' field.
   * @return Id of TaxPayer
   */
  public java.lang.String getId() {
    return id;
  }



  /**
   * Gets the value of the 'name' field.
   * @return Name of TaxPayer
   */
  public java.lang.String getName() {
    return name;
  }



  /**
   * Gets the value of the 'document' field.
   * @return Document of TaxPayer
   */
  public java.lang.String getDocument() {
    return document;
  }



  /**
   * Gets the value of the 'situation' field.
   * @return Legal situation of TaxPayer
   */
  public boolean getSituation() {
    return situation;
  }



  /**
   * Creates a new TaxPayer RecordBuilder.
   * @return A new TaxPayer RecordBuilder
   */
  public static TaxPayer.Builder newBuilder() {
    return new TaxPayer.Builder();
  }

  /**
   * Creates a new TaxPayer RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new TaxPayer RecordBuilder
   */
  public static TaxPayer.Builder newBuilder(TaxPayer.Builder other) {
    if (other == null) {
      return new TaxPayer.Builder();
    } else {
      return new TaxPayer.Builder(other);
    }
  }

  /**
   * Creates a new TaxPayer RecordBuilder by copying an existing TaxPayer instance.
   * @param other The existing instance to copy.
   * @return A new TaxPayer RecordBuilder
   */
  public static TaxPayer.Builder newBuilder(TaxPayer other) {
    if (other == null) {
      return new TaxPayer.Builder();
    } else {
      return new TaxPayer.Builder(other);
    }
  }

  /**
   * RecordBuilder for TaxPayer instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<TaxPayer>
    implements org.apache.avro.data.RecordBuilder<TaxPayer> {

    /** Id of TaxPayer */
    private java.lang.String id;
    /** Name of TaxPayer */
    private java.lang.String name;
    /** Document of TaxPayer */
    private java.lang.String document;
    /** Legal situation of TaxPayer */
    private boolean situation;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(TaxPayer.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.name)) {
        this.name = data().deepCopy(fields()[1].schema(), other.name);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.document)) {
        this.document = data().deepCopy(fields()[2].schema(), other.document);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.situation)) {
        this.situation = data().deepCopy(fields()[3].schema(), other.situation);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing TaxPayer instance
     * @param other The existing instance to copy.
     */
    private Builder(TaxPayer other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.name)) {
        this.name = data().deepCopy(fields()[1].schema(), other.name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.document)) {
        this.document = data().deepCopy(fields()[2].schema(), other.document);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.situation)) {
        this.situation = data().deepCopy(fields()[3].schema(), other.situation);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'id' field.
      * Id of TaxPayer
      * @return The value.
      */
    public java.lang.String getId() {
      return id;
    }


    /**
      * Sets the value of the 'id' field.
      * Id of TaxPayer
      * @param value The value of 'id'.
      * @return This builder.
      */
    public TaxPayer.Builder setId(java.lang.String value) {
      validate(fields()[0], value);
      this.id = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'id' field has been set.
      * Id of TaxPayer
      * @return True if the 'id' field has been set, false otherwise.
      */
    public boolean hasId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'id' field.
      * Id of TaxPayer
      * @return This builder.
      */
    public TaxPayer.Builder clearId() {
      id = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'name' field.
      * Name of TaxPayer
      * @return The value.
      */
    public java.lang.String getName() {
      return name;
    }


    /**
      * Sets the value of the 'name' field.
      * Name of TaxPayer
      * @param value The value of 'name'.
      * @return This builder.
      */
    public TaxPayer.Builder setName(java.lang.String value) {
      validate(fields()[1], value);
      this.name = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'name' field has been set.
      * Name of TaxPayer
      * @return True if the 'name' field has been set, false otherwise.
      */
    public boolean hasName() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'name' field.
      * Name of TaxPayer
      * @return This builder.
      */
    public TaxPayer.Builder clearName() {
      name = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'document' field.
      * Document of TaxPayer
      * @return The value.
      */
    public java.lang.String getDocument() {
      return document;
    }


    /**
      * Sets the value of the 'document' field.
      * Document of TaxPayer
      * @param value The value of 'document'.
      * @return This builder.
      */
    public TaxPayer.Builder setDocument(java.lang.String value) {
      validate(fields()[2], value);
      this.document = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'document' field has been set.
      * Document of TaxPayer
      * @return True if the 'document' field has been set, false otherwise.
      */
    public boolean hasDocument() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'document' field.
      * Document of TaxPayer
      * @return This builder.
      */
    public TaxPayer.Builder clearDocument() {
      document = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'situation' field.
      * Legal situation of TaxPayer
      * @return The value.
      */
    public boolean getSituation() {
      return situation;
    }


    /**
      * Sets the value of the 'situation' field.
      * Legal situation of TaxPayer
      * @param value The value of 'situation'.
      * @return This builder.
      */
    public TaxPayer.Builder setSituation(boolean value) {
      validate(fields()[3], value);
      this.situation = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'situation' field has been set.
      * Legal situation of TaxPayer
      * @return True if the 'situation' field has been set, false otherwise.
      */
    public boolean hasSituation() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'situation' field.
      * Legal situation of TaxPayer
      * @return This builder.
      */
    public TaxPayer.Builder clearSituation() {
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TaxPayer build() {
      try {
        TaxPayer record = new TaxPayer();
        record.id = fieldSetFlags()[0] ? this.id : (java.lang.String) defaultValue(fields()[0]);
        record.name = fieldSetFlags()[1] ? this.name : (java.lang.String) defaultValue(fields()[1]);
        record.document = fieldSetFlags()[2] ? this.document : (java.lang.String) defaultValue(fields()[2]);
        record.situation = fieldSetFlags()[3] ? this.situation : (java.lang.Boolean) defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<TaxPayer>
    WRITER$ = (org.apache.avro.io.DatumWriter<TaxPayer>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<TaxPayer>
    READER$ = (org.apache.avro.io.DatumReader<TaxPayer>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.id);

    out.writeString(this.name);

    out.writeString(this.document);

    out.writeBoolean(this.situation);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.id = in.readString();

      this.name = in.readString();

      this.document = in.readString();

      this.situation = in.readBoolean();

    } else {
      for (int i = 0; i < 4; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.id = in.readString();
          break;

        case 1:
          this.name = in.readString();
          break;

        case 2:
          this.document = in.readString();
          break;

        case 3:
          this.situation = in.readBoolean();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}