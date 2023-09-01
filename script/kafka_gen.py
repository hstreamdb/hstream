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
    "array": "!(KaArray {})",
}

GLOBAL_API_VERSION_PATCH = (0, 0)
API_VERSION_PATCHES = {
    "ApiVersions": (0, 2),
    "Metadata": (0, 1),
    "Produce": (0, 0),
}

# Variables
DATA_TYPES = []
SUB_DATA_TYPES = []
# {(api_key, min_version, max_version, api_name)}
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


class HsData:
    def __init__(
        self,
        name,
        fields: List[HsDataField] | int,
        version,
        cons=None,
        doc=None,
    ):
        self.name = name
        self.fields = fields
        self.version = version
        self.doc = doc
        self._name = name + f"V{version}"
        self._cons = cons + f"V{version} " if cons else self._name

    def format(self):
        if isinstance(self.fields, int):
            # Maybe unused
            return f"type {self._name} = {self.name}V{self.fields}"

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
        derivings = " deriving (Show, Generic)"
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
            if not same_found and data.fields == data_.fields:
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
    if min_version is None:
        raise Exception("min version is None")
    if min_version > version:
        return False
    if max_version is not None and max_version < version:
        return False
    return True


# -----------------------------------------------------------------------------
# Parsers


def parse_version(spec):
    match = re.match(r"^(?P<min>\d+)$", spec)
    if match:
        min_ = int(match.group("min"))
        return min_, min_

    match = re.match(r"^(?P<min>\d+)\+$", spec)
    if match:
        min_ = int(match.group("min"))
        return min_, None

    match = re.match(r"^(?P<min>\d+)\-(?P<max>\d+)$", spec)
    if match:
        min_ = int(match.group("min"))
        max_ = int(match.group("max"))
        return min_, max_

    return None, None


def parse_field(field, api_version=0):
    about = field.get("about")  # TODO
    name = RENAMES.get(field["name"], field["name"])
    type_type = field["type"]
    type_name = None
    type_maps = TYPE_MAPS
    with_extra_version_suffix = False

    # Versions
    min_field_version, max_field_version = parse_version(
        field.get("versions", "")
    )
    min_tagged_version, max_tagged_version = parse_version(
        field.get("taggedVersions", "")
    )

    if min_field_version is None:
        if in_version_range(
            api_version, min_tagged_version, max_tagged_version
        ):
            raise NotImplementedError("taggedVersions")
        return

    if not in_version_range(api_version, min_field_version, max_field_version):
        return
    if nullableVersions := field.get("nullableVersions"):
        min_null_version, max_null_version = parse_version(nullableVersions)
        if in_version_range(api_version, min_null_version, max_null_version):
            type_maps = NULLABLE_TYPE_MAPS

    # TODO
    if "flexibleVersions" in field:
        raise NotImplementedError("flexibleVersions")

    # TODO
    if "taggedVersions" in field:
        raise NotImplementedError("taggedVersions")

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
                (parse_field(f, api_version=api_version) for f in sub_fields),
            )
        )
        hs_data = HsData(type_name, data_sub_fields, api_version)
        append_hs_datas(SUB_DATA_TYPES, hs_data)

    data_name = lower_fst(name)

    if with_extra_version_suffix:
        _type_name = f"{type_name}V{api_version}"
        type_name = DATA_TYPE_RENAMES.get(_type_name, _type_name)
    data_type = type_maps[type_type].format(type_name)
    data_field = HsDataField(data_name, data_type, doc=about)
    return data_field


def parse(msg):
    api_key = msg["apiKey"]
    min_api_version, max_api_version = parse_version(msg["validVersions"])
    flex_min_version, flex_max_version = parse_version(msg["flexibleVersions"])
    name = msg["name"]
    fields = msg["fields"]
    api_type = msg["type"]
    api_name = name.removesuffix(upper_fst(api_type))

    # Get api_version
    (glo_min_api_version, glo_max_api_version) = GLOBAL_API_VERSION_PATCH
    if glo_min_api_version > min_api_version:
        min_api_version = glo_min_api_version
    if glo_max_api_version < max_api_version:
        max_api_version = glo_max_api_version
    if api_version_patch := API_VERSION_PATCHES.get(api_name):
        min_api_version = api_version_patch[0]
        max_api_version = api_version_patch[1]

    # TODO
    if flex_min_version in range(
        min_api_version, max_api_version + 1
    ) or flex_max_version in range(min_api_version, max_api_version + 1):
        raise NotImplementedError("flexibleVersions")

    api = (api_key, min_api_version, max_api_version, api_name)
    for k, minv, maxv, _ in API_VERSIONS:
        if api_key == k:
            assert min_api_version == minv
            assert max_api_version == maxv
    API_VERSIONS.add(api)

    for v in range(min_api_version, max_api_version + 1):
        fs = list(filter(None, (parse_field(f, api_version=v) for f in fields)))
        hs_data = HsData(name, fs, v)
        append_hs_datas(DATA_TYPES, hs_data)


# -----------------------------------------------------------------------------


def gen_header():
    return """
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE TypeFamilies          #-}

-------------------------------------------------------------------------------
-- TODO: Generate by kafka message json schema

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
    for k, _, _, n in sorted(API_VERSIONS):
        api_keys.append(f'  show (ApiKey {k}) = "{n}({k})"')
    api_keys.append('  show (ApiKey n) = "Unknown " <> show n')
    return "\n".join(api_keys)


def gen_supported_api_versions():
    result = "supportedApiVersions :: [ApiVersionV0]\n"
    result += "supportedApiVersions =\n"
    result += format_hs_list(
        (
            f"ApiVersionV0 (ApiKey {k}) {minv} {maxv}"
            for (k, minv, maxv, _) in sorted(API_VERSIONS)
        ),
        indent=2,
    )
    return result


def gen_services():
    max_api_version = max(x[2] for x in API_VERSIONS)
    services = []
    srv_methods = lambda v: format_hs_list(
        (
            '"' + lower_fst(n) + '"'
            for (k, _, max_v, n) in sorted(API_VERSIONS)
            if v <= max_v
        ),
        indent=4,
        prefix="'",
    )
    method_impl_ins = lambda v: "\n".join(
        f"""\
instance HasMethodImpl {srv_name} "{lower_fst(n)}" where
  type MethodName {srv_name} "{lower_fst(n)}" = "{lower_fst(n)}"
  type MethodKey {srv_name} "{lower_fst(n)}" = {k}
  type MethodVersion {srv_name} "{lower_fst(n)}" = {v}
  type MethodInput {srv_name} "{lower_fst(n)}" = {n}RequestV{v}
  type MethodOutput {srv_name} "{lower_fst(n)}" = {n}ResponseV{v}
"""
        for (k, _, max_v, n) in sorted(API_VERSIONS)
        if v <= max_v
    )

    # for all supported api_version
    for v in range(max_api_version + 1):
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


def gen_struct():
    return f"""
{gen_header()}
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
        default="./common/kafka/message",
        dest="files",
    )
    # TODO: since python3.9 there is BooleanOptionalAction available in argparse
    parser_run.add_argument(
        "--no-format",
        action="store_true",
        help="Don't run stylish-haskell to format the result",
    )

    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    if args.sub_command == "run":
        run_parse(args.files)
        if not args.no_format:
            result = subprocess.run(
                "stylish-haskell",
                input=gen_struct().encode(),
                stdout=subprocess.PIPE,
            )
            if result and result.stdout:
                print(result.stdout.decode().strip())
        else:
            print(gen_struct())
