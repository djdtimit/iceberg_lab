"""Neutral source: tables written with Iceberg's Java core API (the reference implementation).

Every engine of the lab is under test, so none of them can be the neutral writer. The neutral
source is the Iceberg Java library itself, called directly through the JVM that PySpark hosts
(py4j) - no Spark SQL, no Spark write path, no Spark table cache:

* its own REST catalog client (``CatalogUtil.buildIcebergCatalog``, same properties as the
  lab's Spark catalogs: URI, warehouse, catalog authentication, vended credentials);
* data files via ``GenericFileWriterFactory`` (Parquet), commits via the core table API
  (``newAppend``, ``newOverwrite``, ``newRowDelta``, ``newRewrite``, transactions, ...);
* delete files of every kind the spec has: v2 position deletes, v3 deletion vectors
  (``BaseDVFileWriter``, Puffin) and equality deletes;
* native format v3 (row lineage is assigned by the core library on commit).

This writes every state an engine can produce, also those PyIceberg 0.12 cannot (v3 data,
delete files, nanosecond timestamps, ``unknown``, geo, ``variant``, NULLs in ``list<struct>``). It shares code with the Java engines' WRITE paths (Spark, Flink, Trino's
metadata), not with any engine's read path - a reader that fails here fails on a table built
by the spec's reference implementation.
"""

from __future__ import annotations

import datetime as dt
import json

from pyiceberg.partitioning import PartitionSpec as PySpec
from pyiceberg.schema import Schema as PySchema
from pyiceberg.types import DoubleType, IntegerType, LongType, StringType, TimestampType

from iceberg_lab.config import SETTINGS, catalog_auth_props


class ReferenceWriter:
    """Writes Iceberg tables through the Java core API in ``spark``'s JVM (see module doc)."""

    def __init__(self, spark, warehouse: str):
        """Build the Java REST catalog client with the lab's Spark catalog properties.

        Args:
            spark: SparkSession that hosts the JVM.
            warehouse: Warehouse to write into.
        """
        self.spark = spark
        self.jvm = spark._jvm
        self.gw = spark.sparkContext._gateway
        self.ice = self.jvm.org.apache.iceberg
        props = {
            "type": "rest",
            "uri": SETTINGS.catalog_uri,
            "warehouse": warehouse,
            **catalog_auth_props(),
        }
        jprops = self.jvm.java.util.HashMap()
        for k, v in props.items():
            jprops.put(k, v)
        self.catalog = self.ice.CatalogUtil.buildIcebergCatalog(
            "lab_reference", jprops, spark._jsc.hadoopConfiguration()
        )
        self._task = 0
        jackson = self.jvm.com.fasterxml.jackson.databind
        self._json = jackson.ObjectMapper().enable(jackson.DeserializationFeature.USE_LONG_FOR_INTS)
        self._map_class = self.jvm.java.lang.Class.forName("java.util.HashMap")

    # ------------------------------------------------------------------ table handling

    def ident(self, ns: str, name: str):
        """Java TableIdentifier.

        Args:
            ns: Namespace.
            name: Table name.

        Returns:
            The identifier.
        """
        return self.ice.catalog.TableIdentifier.parse(f"{ns}.{name}")

    def create(self, ns: str, name: str, schema: PySchema, fv: int, spec: PySpec | None = None):
        """Create a table natively in format ``fv``.

        Args:
            ns: Namespace.
            name: Table name.
            schema: PyIceberg schema (converted to Java).
            fv: Iceberg format version (2 or 3).
            spec: PyIceberg partition spec (None: unpartitioned).

        Returns:
            The Java table.
        """
        jschema = self.ice.SchemaParser.fromJson(schema.model_dump_json())
        jspec = (
            self.ice.PartitionSpecParser.fromJson(jschema, spec.model_dump_json())
            if spec is not None and spec.fields
            else self.ice.PartitionSpec.unpartitioned()
        )
        props = self.jvm.java.util.HashMap()
        props.put("format-version", str(fv))
        return self.catalog.createTable(self.ident(ns, name), jschema, jspec, props)

    def load(self, ns: str, name: str):
        """Load a table.

        Args:
            ns: Namespace.
            name: Table name.

        Returns:
            The Java table.
        """
        return self.catalog.loadTable(self.ident(ns, name))

    # ------------------------------------------------------------------ records

    def _value(self, field_type, v):
        """Python value -> Java object of the field's type (Long via Jackson, see ``record``).

        Args:
            field_type: PyIceberg type of the field.
            v: Python value.

        Returns:
            The Java value.
        """
        if v is None:
            return None
        if isinstance(field_type, IntegerType):
            return self.jvm.java.lang.Integer.valueOf(str(v))
        if isinstance(field_type, DoubleType):
            return self.jvm.java.lang.Double.valueOf(repr(float(v)))
        if isinstance(field_type, StringType):
            return str(v)
        if isinstance(field_type, TimestampType):
            assert isinstance(v, dt.datetime) and v.tzinfo is None
            return self.jvm.java.time.LocalDateTime.parse(v.isoformat())
        raise TypeError(f"reference writer: no conversion for {field_type}")

    def record(self, table, row: tuple, schema=None):
        """GenericRecord of ``row`` (values in schema column order).

        Args:
            table: Java Iceberg table.
            row: Values in column order.
            schema: Java schema (default: the table's).

        Returns:
            The GenericRecord.
        """
        jschema = schema or table.schema()
        py = PySchema.model_validate(json.loads(self.ice.SchemaParser.toJson(jschema)))
        rec = self.ice.data.GenericRecord.create(jschema)
        longs = {}
        for field, v in zip(py.fields, row, strict=True):
            if isinstance(field.field_type, LongType) and v is not None:
                longs[field.name] = int(v)
            else:
                rec.setField(field.name, self._value(field.field_type, v))
        if longs:
            # py4j turns every java.lang.Long it gets back into a Python int and sends small
            # ints as Integer - a Long field would get an Integer (ClassCastException in the
            # Parquet writer). Jackson with USE_LONG_FOR_INTS builds the Longs inside the JVM.
            rec = rec.copy(self._json.readValue(json.dumps(longs), self._map_class))
        return rec

    # ------------------------------------------------------------------ file writers

    def _files(self, table, fmt: str = "PARQUET"):
        """OutputFileFactory for new files of the table.

        Args:
            table: Java Iceberg table.
            fmt: File format (``PARQUET``, ``PUFFIN``).

        Returns:
            The factory.
        """
        self._task += 1
        return (
            self.ice.io.OutputFileFactory.builderFor(table, 1, self._task)
            .format(getattr(self.ice.FileFormat, fmt))
            .build()
        )

    def _factory(self, table, eq_field: str | None = None):
        """GenericFileWriterFactory (Parquet data and delete files).

        Args:
            table: Java Iceberg table.
            eq_field: Column of equality deletes (None: none).

        Returns:
            The factory.
        """
        b = (
            getattr(self.ice.data, "GenericFileWriterFactory$Builder")(table)
            .dataSchema(table.schema())
            .dataFileFormat(self.ice.FileFormat.PARQUET)
            .deleteFileFormat(self.ice.FileFormat.PARQUET)
        )
        if eq_field is not None:
            ids = self.gw.new_array(self.jvm.int, 1)
            ids[0] = table.schema().findField(eq_field).fieldId()
            cols = self.jvm.java.util.ArrayList()
            cols.add(eq_field)
            b = b.equalityFieldIds(ids).equalityDeleteRowSchema(table.schema().select(cols))
        return b.build()

    def data_files(self, table, rows: list[tuple]) -> list:
        """One Parquet data file per partition of the CURRENT spec, rows in the given order
        (position of a row in its file = its index among the rows of that partition).

        Args:
            table: Java Iceberg table.
            rows: Rows in column order.

        Returns:
            The Java DataFiles.
        """
        spec, schema = table.spec(), table.schema()
        factory, files = self._factory(table), self._files(table)
        groups: dict[str, tuple] = {}
        if spec.isUnpartitioned():
            groups[""] = (None, [self.record(table, r) for r in rows])
        else:
            wrapper = self.ice.data.InternalRecordWrapper(schema.asStruct())
            for r in rows:
                rec = self.record(table, r)
                pk = self.ice.PartitionKey(spec, schema)
                pk.partition(wrapper.wrap(rec))
                groups.setdefault(pk.toPath(), (pk.copy(), []))[1].append(rec)
        out = []
        for partition, recs in groups.values():
            target = (
                files.newOutputFile() if partition is None else files.newOutputFile(spec, partition)
            )
            w = factory.newDataWriter(target, spec, partition)
            for rec in recs:
                w.write(rec)
            w.close()
            out.append(w.toDataFile())
        return out

    def position_deletes(self, table, data_file, positions: list[int]):
        """Delete files for rows ``positions`` of ``data_file``: a v3 deletion vector (Puffin)
        or a v2 position delete file - what the spec prescribes for the table's version.

        Args:
            table: Java Iceberg table.
            data_file: Java DataFile the deletes point at.
            positions: Row positions to delete.

        Returns:
            The Java DeleteFiles.
        """
        spec = table.specs().get(data_file.specId())
        partition = data_file.partition() if not spec.isUnpartitioned() else None
        path = data_file.location()
        if self.ice.TableUtil.formatVersion(table) >= 3:
            # loadPreviousDeletes: no earlier deletes for these files (a Python object that
            # implements java.util.function.Function, via py4j's callback server).
            w = self.ice.deletes.BaseDVFileWriter(self._files(table, "PUFFIN"), self._no_deletes())
            for pos in positions:
                w.delete(path, self.jvm.java.lang.Long.valueOf(str(pos)), spec, partition)
            w.close()
            return list(w.result().deleteFiles())
        files = self._files(table)
        target = (
            files.newOutputFile() if partition is None else files.newOutputFile(spec, partition)
        )
        w = self._factory(table).newPositionDeleteWriter(target, spec, partition)
        for pos in positions:
            pd = self.ice.deletes.PositionDelete.create()
            pd.set(path, pos)
            w.write(pd)
        w.close()
        return [w.toDeleteFile()]

    def equality_deletes(self, table, field: str, values: list):
        """One equality delete file: rows whose ``field`` equals one of ``values``.

        Args:
            table: Java Iceberg table.
            field: Equality column.
            values: Values to delete.

        Returns:
            The Java DeleteFile.
        """
        del_schema = table.schema().select(self._list([field]))
        files = self._files(table)
        w = self._factory(table, field).newEqualityDeleteWriter(
            files.newOutputFile(), table.spec(), None
        )
        for v in values:
            w.write(self.record(table, (v,), del_schema))
        w.close()
        return w.toDeleteFile()

    def _no_deletes(self):
        """py4j callback ``Function`` that knows no earlier deletes (callback server started).

        Returns:
            The callback.
        """
        from pyspark.java_gateway import ensure_callback_server_started

        ensure_callback_server_started(self.gw)
        return _NoPreviousDeletes()

    def _list(self, items):
        """Python list -> java.util.ArrayList.

        Args:
            items: Python items.

        Returns:
            The Java list.
        """
        jl = self.jvm.java.util.ArrayList()
        for i in items:
            jl.add(i)
        return jl

    # ------------------------------------------------------------------ commits

    def append(self, table, rows: list[tuple]) -> list:
        """Append the rows (one commit).

        Args:
            table: Java Iceberg table.
            rows: Rows in column order.

        Returns:
            The new DataFiles.
        """
        files = self.data_files(table, rows)
        op = table.newAppend()
        for f in files:
            op.appendFile(f)
        op.commit()
        return files

    def replace(self, table, old: list, new_rows: list[tuple]) -> list:
        """Copy-on-write: ``old`` data files replaced by files with ``new_rows`` (one commit).

        Args:
            table: Java Iceberg table.
            old: Java DataFiles to replace.
            new_rows: Rows of the new files.

        Returns:
            The new DataFiles.
        """
        new = self.data_files(table, new_rows) if new_rows else []
        op = table.newOverwrite()
        for f in old:
            op.deleteFile(f)
        for f in new:
            op.addFile(f)
        op.commit()
        return new

    def row_delta(self, table, rows: list[tuple], deletes: list) -> None:
        """One RowDelta commit: new rows plus delete files.

        Args:
            table: Java Iceberg table.
            rows: Rows in column order.
            deletes: Java DeleteFiles.
        """
        op = table.newRowDelta()
        for f in self.data_files(table, rows) if rows else []:
            op.addRows(f)
        for d in deletes:
            op.addDeletes(d)
        op.commit()

    def live_files(self, table) -> tuple[list, list]:
        """(data files, delete files) of the current snapshot.

        Args:
            table: Java table.

        Returns:
            ``(data files, delete files)``.
        """
        data, deletes = [], []
        it = table.newScan().planFiles().iterator()
        while it.hasNext():
            task = it.next()
            data.append(task.file())
            deletes.extend(task.deletes())
        it.close()
        seen, unique = set(), []
        for d in deletes:
            if d.location() not in seen:
                seen.add(d.location())
                unique.append(d)
        return data, unique

    # ------------------------------------------------------------------ any type

    def write_values(self, ns: str, name: str, type_json, fv: int, rows: list[tuple]):
        """Create table ``(id long, v <type>)`` natively in format ``fv`` and append ``rows``.

        Covers every Iceberg type, also those PyIceberg 0.12 cannot write: nanosecond
        timestamps, ``unknown``, ``geometry``/``geography`` (WKB), ``variant`` and NULLs
        inside ``list<struct>``.

        Args:
            ns: Namespace.
            name: Table name.
            type_json: Iceberg JSON of the column type (``"variant"``, ``"timestamp_ns"``,
                a list/struct object, ...).
            fv: Iceberg format version (2 or 3).
            rows: ``(id, python value)`` pairs (see ``_jvalue`` for the value forms).

        Returns:
            The Java table.
        """
        schema = self.ice.SchemaParser.fromJson(
            json.dumps(
                {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "required": False, "type": "long"},
                        {"id": 2, "name": "v", "required": False, "type": type_json},
                    ],
                }
            )
        )
        props = self.jvm.java.util.HashMap()
        props.put("format-version", str(fv))
        try:
            table = self.catalog.createTable(
                self.ident(ns, name), schema, self.ice.PartitionSpec.unpartitioned(), props
            )
            vtype = table.schema().findField("v").type()
            writer = self._factory(table).newDataWriter(
                self._files(table).newOutputFile(), table.spec(), None
            )
            for i, value in rows:
                rec = self.ice.data.GenericRecord.create(table.schema())
                rec.setField("v", self._jvalue(vtype, value))
                # id is a Long: built inside the JVM (see ``record``)
                writer.write(rec.copy(self._json.readValue(json.dumps({"id": i}), self._map_class)))
            writer.close()
            table.newAppend().appendFile(writer.toDataFile()).commit()
        except Exception as e:
            java = getattr(e, "java_exception", None)
            if java is None:
                raise
            # py4j's message is only "An error occurred while calling ..."; the catalog's answer
            # (status, error type, message) is in the Java exception - gap patterns match on it.
            raise JavaError(str(java.toString())) from e
        return table

    def _jvalue(self, jtype, v):
        """Python value -> Java value of the Iceberg type ``jtype`` (recursive).

        Forms: timestamps as ``pandas.Timestamp``/``datetime`` (ISO with nanoseconds), geo as
        WKB ``bytes``, lists as ``list``, structs as ``dict``, variant as JSON-like Python
        (dict, list, bool, int, float, str, None).

        Args:
            jtype: Java Iceberg type.
            v: Python value.

        Returns:
            The Java value.
        """
        if v is None:
            return None
        tid = jtype.typeId().name()
        if tid == "TIMESTAMP_NANO":
            iso = v.isoformat()
            if jtype.shouldAdjustToUTC():
                return self.jvm.java.time.OffsetDateTime.parse(iso)
            return self.jvm.java.time.LocalDateTime.parse(iso)
        if tid in ("GEOMETRY", "GEOGRAPHY", "BINARY"):
            return self.jvm.java.nio.ByteBuffer.wrap(bytes(v))
        if tid == "LIST":
            out = self.jvm.java.util.ArrayList()
            for item in v:
                out.add(self._jvalue(jtype.elementType(), item))
            return out
        if tid == "STRUCT":
            rec = self.ice.data.GenericRecord.create(jtype)
            for f in jtype.fields():
                rec.setField(f.name(), self._jvalue(f.type(), v.get(f.name())))
            return rec
        if tid == "INTEGER":
            return self.jvm.java.lang.Integer.valueOf(str(v))
        if tid == "STRING":
            return str(v)
        if tid == "VARIANT":
            return self._variant(v)
        raise TypeError(f"reference writer: no conversion for {tid}")

    def _variant(self, value):
        """JSON-like Python value -> Java ``Variant`` (metadata holds every object key).

        Args:
            value: dict, list, bool, int, float, str or None (nested).

        Returns:
            The Java Variant.
        """
        variants = self.jvm.org.apache.iceberg.variants
        names: list[str] = []

        def keys(x):
            """Collect the object keys of ``x`` (recursive) into ``names``.

            Args:
                x: Python value.
            """
            if isinstance(x, dict):
                for k, item in x.items():
                    if k not in names:
                        names.append(k)
                    keys(item)
            elif isinstance(x, list):
                for item in x:
                    keys(item)

        keys(value)
        arr = self.gw.new_array(self.jvm.String, len(names))
        for i, n in enumerate(names):
            arr[i] = n
        metadata = variants.Variants.metadata(arr)

        def build(x):
            """Python value -> Java VariantValue.

            Args:
                x: Python value.

            Returns:
                The Java VariantValue.
            """
            if x is None:
                return variants.Variants.ofNull()
            if isinstance(x, dict):
                obj = variants.Variants.object(metadata)
                for k, item in x.items():
                    obj.put(k, build(item))
                return obj
            if isinstance(x, list):
                out = variants.Variants.array()
                for item in x:
                    out.add(build(item))
                return out
            if isinstance(x, bool | int | float | str):
                return variants.Variants.of(x)
            raise TypeError(f"reference writer: no variant form for {type(x).__name__}")

        return variants.Variant.of(metadata, build(value))

    def read(self, table) -> list[tuple]:
        """Rows as the reference implementation reads them (source check), sorted.

        Args:
            table: Java table.

        Returns:
            The sorted rows.
        """
        table.refresh()
        names = [f.name() for f in table.schema().columns()]
        it = self.ice.data.IcebergGenerics.read(table).build().iterator()
        rows = []
        while it.hasNext():
            rec = it.next()
            rows.append(tuple(_py(rec.getField(n)) for n in names))
        it.close()
        return sorted(rows, key=lambda r: tuple((v is None, 0 if v is None else v) for v in r))


class JavaError(RuntimeError):
    """An exception of the Java core library, with the Java exception's text as message."""


class _NoPreviousDeletes:
    """java.util.function.Function<String, PositionDeleteIndex> that knows no earlier deletes."""

    def apply(self, path):
        """No earlier deletes for any path.

        Args:
            path: Data file path (unused).
        """
        return

    class Java:
        """py4j: the Java interface this Python object implements."""

        implements = ["java.util.function.Function"]  # noqa: RUF012 - py4j protocol


def _py(v):
    """Java value from py4j -> Python (LocalDateTime -> datetime).

    Args:
        v: Value from py4j.

    Returns:
        The Python value.
    """
    if v is None or isinstance(v, int | float | str):
        return v
    return dt.datetime.fromisoformat(str(v))
