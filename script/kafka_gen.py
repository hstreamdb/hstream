#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

# Parse Apache Kafka Message Definitions
#
# See: https://github.com/apache/kafka/tree/3.5/clients/src/main/resources/common/message
#
# TODO:
#
# - handle ignorable
# - handle entityType

import argparse
from dataclasses import dataclass
import os
import json
from glob import glob
import subprocess
import re
from typing import List, Optional
import textwrap

# If you want to enable tab completion of this script, you must install
# argcomplete, for details, see:
#
# https://kislyuk.github.io/argcomplete/#installation
try:
    import argcomplete
except Exception:
    from unittest.mock import MagicMock

    argcomplete = MagicMock()
    argcomplete.autocomplete = lambda x: None


# -----------------------------------------------------------------------------
# Constants

RENAMES = {"Records": "RecordBytes"}

TYPE_MAPS = {
    # NOTE: double { is required because of python formatting
    "int8": "{{-# UNPACK #-}} !Int8",
    "int32": "{{-# UNPACK #-}} !Int32",
    "int16": "{{-# UNPACK #-}} !Int16",
    "int64": "{{-# UNPACK #-}} !Int64",
    "string": "!Text",
    "bool": "Bool",
    "bytes": "!ByteString",
    "records": "!ByteString",
    "array": "!(KaArray {})",
    "errorCode": "{{-# UNPACK #-}} !ErrorCode",
    "apiKey": "{{-# UNPACK #-}} !ApiKey",
}
NULLABLE_TYPE_MAPS = {
    "string": "!NullableString",
    "bytes": "!NullableBytes",
    "records": "!NullableBytes",
}
COMPACT_TYPE_MAPS = {
    "string": "!CompactString",
    "bytes": "!CompactBytes",
    "records": "!CompactBytes",
    "array": "!(CompactKaArray {})",
}
COMPACT_NULLABLE_TYPE_MAPS = {
    "string": "!CompactNullableString",
    "bytes": "!CompactNullableBytes",
    "records": "!CompactNullableBytes",
    "array": "!(CompactKaArray {})",
}

GLOBAL_API_VERSION_PATCH = (0, 0)
API_VERSION_PATCHES = {
    "ApiVersions": (0, 3),
    "Metadata": (0, 1),
    "Produce": (2, 2),
    "Fetch": (2, 2),
    "OffsetFetch": (0, 2),
    "OffsetCommit": (0, 2),
}

# -----------------------------------------------------------------------------
# Variables


# Since order is True, the fields order is important
@dataclass(eq=True, frozen=True, order=True)
class ApiVersion:
    api_key: int
    api_name: str
    min_version: int
    max_version: int
    min_flex_version: int
    max_flex_version: int


DATA_TYPES = []
SUB_DATA_TYPES = []

# Set of ApiVersion
API_VERSIONS = set()

DATA_TYPE_RENAMES = {}


# -----------------------------------------------------------------------------
# Haskell data types


def format_doc(
    doc=None, width=79, initial_indent="--| ", subsequent_indent="-- "
):
    return "\n".join(
        textwrap.wrap(
            doc or "",
            width=width,
            initial_indent=initial_indent,
            subsequent_indent=subsequent_indent,
        )
    )


def format_field_doc(doc=None, indent=4):
    return format_doc(
        doc=doc,
        initial_indent=" " * indent + "-- ^ ",
        subsequent_indent=" " * indent + "-- ",
    )


def format_hs_list(xs, indent=0, prefix=""):
    indents = " " * indent
    indents_with_prefix = indents + (" " * len(prefix))
    result = indents + prefix + "[ "
    result += ("\n" + indents_with_prefix + ", ").join(xs)
    result += "\n"
    result += indents_with_prefix + "]"
    return result


@dataclass
class HsDataField:
    name: str
    ty: str
    doc: Optional[str] = None
    is_tagged: bool = False


class HsData:
    def __init__(
        self,
        name,
        fields: List[HsDataField] | int,
        version,
        is_flexible=False,
        cons=None,
        doc=None,
    ):
        self.name = name
        self.is_flexible = is_flexible
        self._init_fields(fields)
        self.version = version
        self.doc = doc
        self._name = name + f"V{version}"
        self._cons = cons + f"V{version} " if cons else self._name

    def _init_fields(self, fields):
        self.fields = []
        self.tagged_fields = []

        if isinstance(fields, int):
            self.fields = fields
        else:
            for f in fields:
                if f.is_tagged:
                    self.tagged_fields.append(f)
                else:
                    self.fields.append(f)

    def format(self):
        if isinstance(self.fields, int):
            # Maybe unused
            return f"type {self._name} = {self.name}V{self.fields}"

        # TODO: tagged_fields
        #
        # FIXME:
        #
        # 1. We assume that tagged_fields is always the last field
        # 2. We assume that flexible message always has tagged_fields
        if self.tagged_fields or self.is_flexible:
            self.fields.append(
                HsDataField("taggedFields", "!TaggedFields", is_tagged=True)
            )

        if len(self.fields) == 0:
            data_type = f"data {self._name} = {self._cons}"
            data_fields = " "
        elif len(self.fields) == 1:
            data_type = f"newtype {self._name} = {self._cons}"
            data_fields = "\n  , ".join(
                f"{f.name} :: {remove_strict(f.ty)}" for f in self.fields
            )
            data_fields = "  { " + data_fields + "\n  }"
        else:
            data_type = f"data {self._name} = {self._cons}"
            data_fields = "\n  , ".join(
                f"{f.name} :: {f.ty}\n{format_field_doc(f.doc)}"
                if f.doc
                else f"{f.name} :: {f.ty}"
                for f in self.fields
            )
            data_fields = "  { " + data_fields + "\n  }"
        derivings = " deriving (Show, Eq, Generic)"
        derivings += f"\ninstance Serializable {self._name}"

        data_doc = format_doc(self.doc)
        if data_doc:
            data_type = data_doc + "\n" + data_type
        return data_type + "\n" + data_fields + derivings


def append_hs_datas(datas: List[HsData], data: HsData):
    same_found = False
    for data_ in datas:
        if data.name == data_.name:
            # The same data_name should not has the same version
            assert data.version != data_.version

            # Use the first same type is OK
            if (
                not same_found
                and data.is_flexible == data_.is_flexible  # noqa: W503
                and data.fields == data_.fields  # noqa: W503
            ):
                # An int mean use this fileds instead
                DATA_TYPE_RENAMES[
                    f"{data.name}V{data.version}"
                ] = f"{data.name}V{data_.version}"
                data.fields = data_.version
                same_found = True
    datas.append(data)


# -----------------------------------------------------------------------------
# Helpers


def lower_fst(string):
    if string == "":
        return string
    return string[0].lower() + string[1:]


def upper_fst(string):
    if string == "":
        return string
    return string[0].upper() + string[1:]


def remove_strict(string):
    return re.sub(r"({-# UNPACK #-})*\s*\!\s*", "", string)


def load_json_with_comments(path):
    with open(path, "r") as f:

        def lines():
            for line in f.readlines():
                line = re.sub(r"\/\/.*", "", line)
                if line.strip():
                    yield line

        return json.loads("\n".join(lines()))


def in_version_range(version, min_version, max_version):
    if min_version is None or min_version > version:
        return False
    if max_version is not None and max_version < version:
        return False
    return True


# https://github.com/apache/kafka/blob/3.5.1/generator/src/main/java/org/apache/kafka/message/ApiMessageTypeGenerator.java#L329
def get_header_version(v, api):
    resp_version = None
    req_version = None

    in_flex = in_version_range(v, api.min_flex_version, api.max_flex_version)
    if in_flex:
        req_version, resp_version = 2, 1
    else:
        req_version, resp_version = 1, 0

    # Hardcoded Exception: ApiVersionsResponse always includes a v0 header
    if api.api_key == 18:
        resp_version = 0

    # Hardcoded Exception:
    #
    # Version 0 of ControlledShutdownRequest has a non-standard request header
    # which does not include clientId.  Version 1 of ControlledShutdownRequest
    # and later use the standard request header.
    if api.api_key == 7:
        if v == 0:
            req_version = 0

    assert req_version is not None
    assert resp_version is not None
    return (req_version, resp_version)


# -----------------------------------------------------------------------------
# Parsers


def parse_version(spec):
    # e.g. 2 -> (2, 2)
    match = re.match(r"^(?P<min>\d+)$", spec)
    if match:
        min_ = int(match.group("min"))
        return min_, min_

    # e.g. 2+ -> (2, None)
    match = re.match(r"^(?P<min>\d+)\+$", spec)
    if match:
        min_ = int(match.group("min"))
        return min_, None

    # e.g. 1-2 -> (1, 2)
    match = re.match(r"^(?P<min>\d+)\-(?P<max>\d+)$", spec)
    if match:
        min_ = int(match.group("min"))
        max_ = int(match.group("max"))
        return min_, max_

    # invlid
    return None, None


def parse_field(field, api_version=0, flexible=False):
    about = field.get("about")  # TODO
    name = RENAMES.get(field["name"], field["name"])
    type_type = field["type"]
    type_name = None
    type_maps = TYPE_MAPS
    with_extra_version_suffix = False
    is_tagged = False

    # Versions
    min_field_version, max_field_version = parse_version(
        field.get("versions", "")
    )
    min_tagged_version, max_tagged_version = parse_version(
        field.get("taggedVersions", "")
    )
    min_null_version, max_null_version = parse_version(
        field.get("nullableVersions", "")
    )

    # field has no "versions"
    if min_field_version is None:
        # a "taggedVersions" must be present
        assert min_tagged_version is not None

    in_api_version = in_version_range(
        api_version, min_field_version, max_field_version
    )
    in_null_version = in_version_range(
        api_version, min_null_version, max_null_version
    )
    in_tagged_version = in_version_range(
        api_version, min_tagged_version, max_tagged_version
    )

    if (in_api_version, in_tagged_version) == (False, False):
        return
    elif (in_api_version, in_tagged_version) == (True, False):
        pass
    elif (in_api_version, in_tagged_version) == (False, True):
        raise NotImplementedError("Only taggedVersions")
    elif (in_api_version, in_tagged_version) == (True, True):
        is_tagged = True

    # Note that tagged fields can only be added to "flexible" message versions.
    if min_tagged_version is not None:
        assert flexible

    if (flexible, in_null_version) == (True, True):
        type_maps = {**type_maps, **COMPACT_NULLABLE_TYPE_MAPS}
    elif (flexible, in_null_version) == (True, False):
        type_maps = {**type_maps, **COMPACT_TYPE_MAPS}
    elif (flexible, in_null_version) == (False, True):
        type_maps = {**type_maps, **NULLABLE_TYPE_MAPS}

    # XXX, maybe unused since flexibleVersions should not in field level (?)
    if "flexibleVersions" in field:
        raise NotImplementedError("flexibleVersions in field!")

    # Error code
    if name == "ErrorCode" and type_type == "int16":
        type_type = "errorCode"

    if name == "ApiKey" and type_type == "int16":
        type_type = "apiKey"

    # Array type
    match_array = re.match(r"^\[\](?P<type>.+)$", type_type)
    if match_array:
        type_type = "array"
        match_name = match_array.group("type")
        _type_name = TYPE_MAPS.get(match_name)
        if _type_name:
            type_name = remove_strict(_type_name.format())
        else:
            with_extra_version_suffix = True
            type_name = remove_strict(match_name.format())

    # Sub fields
    sub_fields = field.get("fields")
    if sub_fields:
        # TODO: Hash Sets
        #
        # if any("mapKey" in d for d in sub_fields):
        #    type_type = "set"
        data_sub_fields = list(
            filter(
                None,
                (
                    parse_field(f, api_version=api_version, flexible=flexible)
                    for f in sub_fields
                ),
            )
        )
        hs_data = HsData(
            type_name, data_sub_fields, api_version, is_flexible=flexible
        )
        append_hs_datas(SUB_DATA_TYPES, hs_data)

    data_name = lower_fst(name)

    if with_extra_version_suffix:
        _type_name = f"{type_name}V{api_version}"
        type_name = DATA_TYPE_RENAMES.get(_type_name, _type_name)
    data_type = type_maps[type_type].format(type_name)
    data_field = HsDataField(
        data_name,
        data_type,
        doc=about,
        is_tagged=is_tagged,
    )
    return data_field


def parse(msg):
    api_key = msg["apiKey"]
    min_api_version, max_api_version = parse_version(msg["validVersions"])
    min_flex_version, max_flex_version = parse_version(msg["flexibleVersions"])
    name = msg["name"]
    fields = msg["fields"]
    api_type = msg["type"]
    api_name = name.removesuffix(upper_fst(api_type))

    assert api_type in ["request", "response"]

    # Get api_version
    (glo_min_api_version, glo_max_api_version) = GLOBAL_API_VERSION_PATCH
    if glo_min_api_version > min_api_version:
        min_api_version = glo_min_api_version
    if glo_max_api_version < max_api_version:
        max_api_version = glo_max_api_version
    if api_version_patch := API_VERSION_PATCHES.get(api_name):
        min_api_version = api_version_patch[0]
        max_api_version = api_version_patch[1]

    for api in API_VERSIONS:
        if api_key == api.api_key:
            assert min_api_version == api.min_version
            assert max_api_version == api.max_version

    API_VERSIONS.add(
        ApiVersion(
            api_key=api_key,
            api_name=api_name,
            min_version=min_api_version,
            max_version=max_api_version,
            min_flex_version=min_flex_version,
            max_flex_version=max_flex_version,
        )
    )

    for v in range(min_api_version, max_api_version + 1):
        flexible = in_version_range(v, min_flex_version, max_flex_version)
        fs = list(
            filter(
                None,
                (
                    parse_field(f, api_version=v, flexible=flexible)
                    for f in fields
                ),
            )
        )
        hs_data = HsData(name, fs, v, is_flexible=flexible)
        append_hs_datas(DATA_TYPES, hs_data)


# -----------------------------------------------------------------------------


def gen_haskell_header():
    return """
-------------------------------------------------------------------------------
-- Autogenerated by kafka message json schema
--
-- $ ./script/kafka_gen.py run > hstream-kafka/protocol/Kafka/Protocol/Message/Struct.hs
--
-- DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE TypeFamilies          #-}

module Kafka.Protocol.Message.Struct where

import           Data.ByteString               (ByteString)
import           Data.Int
import           Data.Text                     (Text)
import           GHC.Generics

import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Error
import           Kafka.Protocol.Service
""".strip()


def gen_splitter():
    return "\n-------------------------------------------------------------------------------\n"


def gen_sub_data_types():
    return "\n\n".join(d.format() for d in SUB_DATA_TYPES)


def gen_data_types():
    return "\n\n".join(d.format() for d in DATA_TYPES)


def gen_api_keys():
    api_keys = []
    api_keys.append(
        """\
newtype ApiKey = ApiKey Int16
  deriving newtype (Num, Integral, Real, Enum, Ord, Eq, Bounded, Serializable)
"""
    )
    api_keys.append("instance Show ApiKey where")
    for api in sorted(API_VERSIONS):
        api_keys.append(
            f'  show (ApiKey {api.api_key}) = "{api.api_name}({api.api_key})"'
        )
    api_keys.append('  show (ApiKey n) = "Unknown " <> show n')
    return "\n".join(api_keys)


def gen_supported_api_versions():
    result = "supportedApiVersions :: [ApiVersionV0]\n"
    result += "supportedApiVersions =\n"
    result += format_hs_list(
        (
            f"ApiVersionV0 (ApiKey {api.api_key}) {api.min_version} {api.max_version}"
            for api in sorted(API_VERSIONS)
        ),
        indent=2,
    )
    return result


def gen_services():
    services = []
    srv_methods = lambda v: format_hs_list(
        (
            '"' + lower_fst(api.api_name) + '"'
            for api in sorted(API_VERSIONS)
            if api.min_version <= v <= api.max_version
        ),
        indent=4,
        prefix="'",
    )
    method_impl_ins = lambda v: "\n".join(
        f"""\
instance HasMethodImpl {srv_name} "{lower_fst(api.api_name)}" where
  type MethodName {srv_name} "{lower_fst(api.api_name)}" = "{lower_fst(api.api_name)}"
  type MethodKey {srv_name} "{lower_fst(api.api_name)}" = {api.api_key}
  type MethodVersion {srv_name} "{lower_fst(api.api_name)}" = {v}
  type MethodInput {srv_name} "{lower_fst(api.api_name)}" = {api.api_name}RequestV{v}
  type MethodOutput {srv_name} "{lower_fst(api.api_name)}" = {api.api_name}ResponseV{v}
"""
        for api in sorted(API_VERSIONS)
        if api.min_version <= v <= api.max_version
    )

    # for all supported api_version
    _glo_max_version = max(x.max_version for x in API_VERSIONS)
    _glo_min_version = min(x.min_version for x in API_VERSIONS)
    for v in range(_glo_min_version, _glo_max_version + 1):
        srv_name = f"HStreamKafkaV{v}"
        srv = f"""
data {srv_name}

instance Service {srv_name} where
  type ServiceName {srv_name} = "{srv_name}"
  type ServiceMethods {srv_name} =
{srv_methods(v)}

{method_impl_ins(v)}"""
        services.append(srv)

    return "".join(services).strip()


def gen_api_header_version():
    hs_type = "getHeaderVersion :: ApiKey -> Int16 -> (Int16, Int16)"
    hs_impl = "\n".join(
        f"getHeaderVersion (ApiKey {api.api_key}) {v} = {get_header_version(v, api)}"
        for api in sorted(API_VERSIONS)
        for v in range(api.min_version, api.max_version + 1)
    )
    hs_math_other = (
        'getHeaderVersion k v = error $ "Unknown " <> show k <> " v" <> show v'
    )
    hs_inline = "{-# INLINE getHeaderVersion #-}"
    return f"{hs_type}\n{hs_impl}\n{hs_math_other}\n{hs_inline}"


def gen_struct():
    return f"""
{gen_haskell_header()}
\
{gen_splitter()}
\
{gen_sub_data_types()}
\
{gen_splitter()}
\
{gen_data_types()}
\
{gen_splitter()}
\
{gen_services()}
\
{gen_splitter()}
\
{gen_api_keys()}

{gen_supported_api_versions()}

{gen_api_header_version()}
""".strip()


# -----------------------------------------------------------------------------


def run_parse(files):
    for f in files:
        obj = load_json_with_comments(f)
        parse(obj)


def cli_get_json(path):
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"{path} not exists!")

    files = []
    if os.path.isdir(path):
        files = sorted(glob(f"{path}/*.json"))
    elif os.path.isfile(path):
        files = [path]
    else:
        raise argparse.ArgumentTypeError(f"{path} invalid!")

    return files


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Kafka Message Generator",
        description="Generate Haskell data types from Kafka json schema",
        epilog="Text at the bottom of help",
    )
    subparsers = parser.add_subparsers(dest="sub_command")

    parser_run = subparsers.add_parser("run", help="Parse and print")
    parser_run.add_argument(
        "--path",
        type=cli_get_json,
        help=(
            "Path can be both directory and file, a directory mean parse all "
            "*.json files under it. (Default: %(default)s)"
        ),
        default="./hstream-kafka/message",
        dest="files",
    )
    # TODO: since python3.9 there is BooleanOptionalAction available in argparse
    parser_run.add_argument(
        "--no-format",
        action="store_true",
        help="Don't run stylish-haskell to format the result",
    )
    parser_run.add_argument(
        "--dry-run",
        action="store_true",
        help="perform a trial run with no outputs",
    )

    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    if args.sub_command == "run":
        run_parse(args.files)
        outputs = gen_struct()
        if not args.dry_run:
            if not args.no_format:
                result = subprocess.run(
                    "stylish-haskell",
                    input=outputs.encode(),
                    stdout=subprocess.PIPE,
                )
                if result and result.stdout:
                    print(result.stdout.decode().strip())
            else:
                print(outputs)
