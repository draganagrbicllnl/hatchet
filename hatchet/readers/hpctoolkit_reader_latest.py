import os
import re
import struct
from typing import Dict, Union

import pandas as pd

from hatchet.graphframe import Frame, Graph, GraphFrame, Node


def safe_unpack(
    format: str, data: bytes, offset: int, index: int = None, index_length: int = None
) -> tuple:
    length = struct.calcsize(format)
    if index:
        offset += index * (length if index_length is None else index_length)
    return struct.unpack(format, data[offset : offset + length])


def read_string(data: bytes, offset: int) -> str:
    result = ""
    while True:
        (letter,) = struct.unpack("<c", data[offset : offset + 1])
        letter = letter.decode("ascii")
        if letter == "\x00":
            return result
        result += letter
        offset += 1


METRIC_SCOPE_MAPPING = {
    "execution": "i",
    "function": "e",
    "point": "p",
    "lex_aware": "c",
}

NODE_TYPE_MAPPING = {0: "function", 1: "loop", 2: "line", 3: "instruction"}

FILE_HEADER_OFFSET = 16


class HPCToolkitReaderLatest:

    def __init__(self, dir_path: str, parse_depth: int = None, parent_percentage_time: int = None, application_time: int = None) -> None:
        # TODO: validate parse depth - should be greater than 0/1
        # TODO: handle the case when time_metric_id is not set

        self._parent_percentage = parent_percentage_time
        self._application_percentage = application_time

        self._dir_path = dir_path
        self._parse_depth = parse_depth

        self._meta_file = None
        self._profile_file = None

        self._functions = {}
        self._source_files = {}
        self._load_modules = {}
        self._metric_descriptions = {}
        self._summary_profile = {}
        self._metrics_table = []

        self._time_metric = None
        self._inclusive_metrics = {}
        self._exclusive_metrics = {}

        for file_path in os.listdir(self._dir_path):
            if file_path.split(".")[-1] == "db":
                file_path = os.path.join(self._dir_path, file_path)
                with open(file_path, "rb") as file:
                    file.seek(10)
                    db = file.read(4)
                try:
                    format = db.decode("ascii")
                    if format == "meta":
                        self._meta_file = file_path
                    elif format == "prof":
                        self._profile_file = file_path
                except:
                    pass

        if self._meta_file is None:
            raise ValueError(f"ERROR: meta.db not found.")

        if self._profile_file is None:
            raise ValueError(f"ERROR: profile.db not found.")

    def _read_metric_descriptions(self) -> None:
        with open(self._meta_file, "rb") as file:
            file.seek(FILE_HEADER_OFFSET + 4 * 8)
            formatMetrics = "<QQ"
            meta_db = file.read(struct.calcsize(formatMetrics))
            (
                szMetrics,
                pMetrics,
            ) = safe_unpack(formatMetrics, meta_db, 0)

            file.seek(pMetrics)
            meta_db = file.read(szMetrics)

        pMetrics_old = pMetrics
        (pMetrics, nMetrics, szMetric, szScopeInst) = safe_unpack("<QLBB", meta_db, 0)

        for i in range(nMetrics):
            (pName, pScopeInsts, _, nScopeInsts) = safe_unpack(
                "<QQQH", meta_db, pMetrics - pMetrics_old, i, szMetric
            )

            name = read_string(meta_db, pName - pMetrics_old).lower().strip()
            unit = None
            if name.endswith(")"):
                name = name[:-1]
                unit = name.split("(")[1].lower().strip()
                name = name.split("(")[0].lower().strip()

            for j in range(nScopeInsts):
                (pScope, propMetricId) = safe_unpack(
                    "<QH", meta_db, pScopeInsts - pMetrics_old, j, szScopeInst
                )
                (pScopeName,) = safe_unpack("<Q", meta_db, pScope - pMetrics_old)
                scope_name = METRIC_SCOPE_MAPPING[
                    read_string(meta_db, pScopeName - pMetrics_old).lower().strip()
                ]

                if name in ["cputime", "realtime", "cycles"] and scope_name == "i":
                    self._time_metric = f"{name} ({scope_name})"

                if scope_name in ["i", "e"]:
                    self._metric_descriptions[propMetricId] = f"{name} ({scope_name})"

    def _parse_source_file(self, meta_db: bytes, pFile: int) -> Dict[str, str]:
        if pFile not in self._source_files:
            (pPath,) = safe_unpack(
                "<Q",
                meta_db,
                pFile + struct.calcsize("<Q"),
            )
            self._source_files[pFile] = {
                "id": pFile,
                "file_path": read_string(meta_db, pPath),
            }

        return self._source_files[pFile]

    def _parse_load_module(self, meta_db: bytes, pModule: int) -> Dict[str, str]:
        if pModule not in self._load_modules:
            (pPath,) = safe_unpack(
                "<Q",
                meta_db,
                pModule + struct.calcsize("<Q"),
            )
            self._load_modules[pModule] = {
                "id": pModule,
                "module_path": read_string(meta_db, pPath),
            }

        return self._load_modules[pModule]

    def _parse_function(
        self, meta_db: bytes, pFunction: int
    ) -> Dict[str, Union[str, int]]:
        if pFunction not in self._functions:
            (pName, pModule, offset, pFile, line) = safe_unpack(
                "<QQQQL", meta_db, pFunction
            )

            name = read_string(meta_db, pName)

            if re.fullmatch(
                "P?MPI_.+",
                name,
            ):
                if name.startswith("P"):
                    name = name[1:]

                name = name[: re.match("^MPI_[a-zA-Z_]+", name).end()]

            self._functions[pFunction] = {
                "id": pFunction,
                "name": name,
                "line": line,
                "offset": offset,
            }
            if pFile:
                self._functions[pFunction]["file_id"] = self._parse_source_file(
                    meta_db, pFile
                )["id"]
            if pModule:
                self._functions[pFunction]["module_id"] = self._parse_load_module(
                    meta_db, pModule
                )["id"]

        return self._functions[pFunction]

    def _parse_context(
        self,
        current_offset: int,
        total_size: int,
        parent: Node,
        meta_db: bytes,
        parent_time: int,
    ) -> None:

        final_offset = current_offset + total_size

        while current_offset < final_offset:
            (szChildren, pChildren, ctxId, _, lexicalType, nFlexWords) = safe_unpack(
                "<QQLHBB", meta_db, current_offset
            )

            try:
                my_time = self._summary_profile[ctxId][self._time_metric]
            except:
                my_time = None

            if my_time is None:
                current_offset += 32 + nFlexWords * 8  # TODO: can I move this up??
                continue

            if self._parent_percentage is not None and my_time / parent_time * 100.0 < self._parent_percentage:
                current_offset += 32 + nFlexWords * 8  # TODO: can I move this up??
                continue

            if self._application_percentage is not None and my_time / self._total_execution_time * 100 < self._application_percentage:
                current_offset += 32 + nFlexWords * 8  # TODO: can I move this up??
                continue


            frame = {
                "id": ctxId,
                "type": NODE_TYPE_MAPPING[lexicalType],
                "depth": parent.frame["depth"] + 1,
            }

            if nFlexWords:
                flex_offset = current_offset + 32

                if lexicalType == 0:
                    (pFunction,) = safe_unpack("<Q", meta_db, flex_offset)
                    frame["name"] = self._parse_function(meta_db, pFunction)["name"]

                elif lexicalType == 3:
                    (pModule, offset) = safe_unpack("<QQ", meta_db, flex_offset)
                    frame["name"] = (
                        f"{self._parse_load_module(meta_db, pModule)['module_path']}:{offset}"
                    )

                else:
                    (pFile, line) = safe_unpack("<QL", meta_db, flex_offset)
                    frame["name"] = (
                        f"{self._parse_source_file(meta_db, pFile)['file_path']}:{line}"
                    )

            node = Node(Frame(frame), parent=parent, hnid=ctxId)
            parent.add_child(node)

            node_value = {
                "node": node,
                "name": f"{frame['type']}: {frame['name']}",
            }

            if ctxId in self._summary_profile:
                node_value.update(self._summary_profile[ctxId])

            self._metrics_table.append(node_value)

            if self._parse_depth is None or frame["depth"] < self._parse_depth:
                
                self._parse_context(
                    pChildren,
                    szChildren,
                    node,
                    meta_db,
                    my_time,
                )

            current_offset += 32 + nFlexWords * 8  # TODO: can I move this up??

    def _read_summary_profile(
        self,
    ) -> None:

        with open(self._profile_file, "rb") as file:
            file.seek(FILE_HEADER_OFFSET)
            formatProfileInfos = "<QQ"
            profile_db = file.read(struct.calcsize(formatProfileInfos))
            (szProfileInfos, pProfileInfos) = safe_unpack(
                formatProfileInfos, profile_db, 0
            )

            file.seek(pProfileInfos)
            profile_db = file.read(szProfileInfos)
            (pProfiles,) = safe_unpack("<Q", profile_db, 0)

            (nValues, pValues, nCtxs, _, pCtxIndices) = safe_unpack(
                "<QQLLQ", profile_db, pProfiles - pProfileInfos
            )

            file.seek(pCtxIndices)
            formatCtxs = "<LQ"
            cct_sub_db = file.read(nCtxs * struct.calcsize(formatCtxs))

            file.seek(pValues)
            formatValues = "<Hd"
            values_sub_db = file.read(nValues * struct.calcsize(formatValues))

            for i in range(nCtxs):
                (ctxId, startIndex) = safe_unpack("<LQ", cct_sub_db, 0, i)
                cct_id = ctxId

                if i == nCtxs - 1:
                    end_index = nValues
                else:
                    (_, end_index) = safe_unpack(formatCtxs, cct_sub_db, 0, i + 1)

                self._summary_profile[cct_id] = {}

                for j in range(startIndex, end_index):
                    (metricId, value) = safe_unpack("<Hd", values_sub_db, 0, j)

                    if metricId in self._metric_descriptions:
                        self._summary_profile[cct_id][
                            self._metric_descriptions[metricId]
                        ] = value

                        if self._metric_descriptions[metricId].endswith("(i)"):
                            self._inclusive_metrics[metricId] = (
                                self._metric_descriptions[metricId]
                            )
                        else:
                            self._exclusive_metrics[metricId] = (
                                self._metric_descriptions[metricId]
                            )

    def _read_cct(
        self,
    ) -> None:
        hatchet_roots = []

        with open(self._meta_file, "rb") as file:
            meta_db = file.read()

        (pContext,) = safe_unpack("<Q", meta_db, FILE_HEADER_OFFSET + 7 * 8)
        (pEntryPoints, nEntryPoints, szEntryPoint) = safe_unpack(
            "<QHB", meta_db, pContext
        )

        for i in range(nEntryPoints):
            (szChildren, pChildren, ctxId, entryPoint) = safe_unpack(
                "<QQLH",
                meta_db,
                pEntryPoints,
                i,
                szEntryPoint,
            )

            if entryPoint != 1:
                continue

            frame = {
                "id": ctxId,
                "type": "entry",
                "name": entryPoint,
                "depth": 0,
            }
            entry_node = Node(Frame(frame), parent=None, hnid=ctxId)

            hatchet_roots.append(entry_node)

            entry_value = {"node": entry_node, "name": "entry"}

            if ctxId in self._summary_profile:
                entry_value.update(self._summary_profile[ctxId])

            self._metrics_table.append(entry_value)

            try:
                my_time = self._summary_profile[ctxId][self._time_metric]
            except:
                my_time = None

            self._total_execution_time = my_time

            self._parse_context(
                pChildren,
                szChildren,
                entry_node,
                meta_db,
                my_time
            )

            table = pd.DataFrame(self._metrics_table).set_index("node")
            
            inclusive_metrics = []
            exclusive_metrics = []

            for im in list(self._inclusive_metrics.values()):
                if im in table.columns.tolist():
                    inclusive_metrics.append(im)

            for em in list(self._exclusive_metrics.values()),:
                if em in table.columns.tolist():
                    exclusive_metrics.append(em)

            graphframe = GraphFrame(
                Graph(hatchet_roots),
                table,
                inc_metrics=inclusive_metrics,
                exc_metrics=exclusive_metrics,
                default_metric=self._time_metric,
            )

            print("DATA IMPORTED")
            return graphframe

    def read(self) -> GraphFrame:
        self._read_metric_descriptions()
        self._read_summary_profile()
        return self._read_cct()
