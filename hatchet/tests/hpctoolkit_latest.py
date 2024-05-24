# Copyright 2017-2023 Lawrence Livermore National Security, LLC and other
# Hatchet Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from hatchet import GraphFrame
from hatchet.node import Node


def test_import_entire_db(data_dir: str) -> None:
    graphframe = GraphFrame.from_hpctoolkit_latest(f"{data_dir}/hpctoolkit-gamess")

    assert len(graphframe.graph.roots) == 1
    assert graphframe.graph.roots[0]._hatchet_nid == 1195
    assert graphframe.graph.roots[0]._depth == 0
    assert graphframe.graph.roots[0].frame["name"] == "entry"
    assert graphframe.graph.roots[0].frame["type"] == "entry"

    assert len(graphframe.dataframe) == 10824
    assert "name" in graphframe.dataframe.columns
    assert "realtime (i)" in graphframe.dataframe.columns
    assert "realtime (e)" in graphframe.dataframe.columns
    assert "gpuop (i)" in graphframe.dataframe.columns
    assert "gker (i)" in graphframe.dataframe.columns
    assert "gxcopy (i)" in graphframe.dataframe.columns
    assert "gxcopy:count (i)" in graphframe.dataframe.columns

    measurements = graphframe.dataframe.loc[Node(None, hnid=1195)]
    assert measurements["name"] == "entry"
    assert round(measurements["realtime (i)"], 2) == 1608.49
    assert round(measurements["gpuop (i)"], 2) == 608.09
    assert round(measurements["gker (i)"], 2) == 608.00
    assert round(measurements["gxcopy (i)"], 2) == 0.09
    assert measurements["gxcopy:count (i)"] == 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=1197)]
    assert measurements["name"] == "gamess_"
    assert round(measurements["realtime (i)"], 2) == 1608.49
    assert round(measurements["gpuop (i)"], 2) == 608.09
    assert round(measurements["gker (i)"], 2) == 608.00
    assert round(measurements["gxcopy (i)"], 2) == 0.09
    assert measurements["gxcopy:count (i)"] == 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=1004)]
    assert measurements["name"] == "[libsci_cray.so.5.0]:0"
    assert round(measurements["realtime (i)"], 2) == 0.08
    assert round(measurements["realtime (e)"], 2) == 0.08

    measurements = graphframe.dataframe.loc[Node(None, hnid=1003)]
    assert measurements["name"] == "[libsci_cray.so.5.0]:0"
    assert round(measurements["realtime (i)"], 2) == 0.08
    assert round(measurements["realtime (e)"], 2) == 0.08


def test_filter_by_max_depth(data_dir: str) -> None:
    graphframe = GraphFrame.from_hpctoolkit_latest(
        f"{data_dir}/hpctoolkit-gamess", max_depth=10
    )

    assert len(graphframe.graph.roots) == 1
    assert graphframe.graph.roots[0]._hatchet_nid == 1195
    assert graphframe.graph.roots[0]._depth == 0
    assert graphframe.graph.roots[0].frame["name"] == "entry"
    assert graphframe.graph.roots[0].frame["type"] == "entry"

    assert len(graphframe.dataframe) == 133
    assert "name" in graphframe.dataframe.columns
    assert "realtime (i)" in graphframe.dataframe.columns
    assert "realtime (e)" in graphframe.dataframe.columns
    assert "gpuop (i)" in graphframe.dataframe.columns
    assert "gker (i)" in graphframe.dataframe.columns
    assert "gxcopy (i)" in graphframe.dataframe.columns
    assert "gxcopy:count (i)" in graphframe.dataframe.columns

    measurements = graphframe.dataframe.loc[Node(None, hnid=1195)]
    assert measurements["name"] == "entry"
    assert round(measurements["realtime (i)"], 2) == 1608.49
    assert round(measurements["gpuop (i)"], 2) == 608.09
    assert round(measurements["gker (i)"], 2) == 608.00
    assert round(measurements["gxcopy (i)"], 2) == 0.09
    assert measurements["gxcopy:count (i)"] == 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=1197)]
    assert measurements["name"] == "gamess_"
    assert round(measurements["realtime (i)"], 2) == 1608.49
    assert round(measurements["gpuop (i)"], 2) == 608.09
    assert round(measurements["gker (i)"], 2) == 608.00
    assert round(measurements["gxcopy (i)"], 2) == 0.09
    assert measurements["gxcopy:count (i)"] == 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=9846)]
    assert measurements["name"] == "wfn_"
    assert round(measurements["realtime (i)"], 2) == 786.09
    assert round(measurements["gpuop (i)"], 2) == 608.09
    assert round(measurements["gker (i)"], 2) == 608.00
    assert round(measurements["gxcopy (i)"], 2) == 0.09
    assert measurements["gxcopy:count (i)"] == 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=9845)]
    assert measurements["name"] == "[gamess.00.x]:0"
    assert round(measurements["realtime (i)"], 2) == 786.09
    assert round(measurements["gpuop (i)"], 2) == 608.09
    assert round(measurements["gker (i)"], 2) == 608.00
    assert round(measurements["gxcopy (i)"], 2) == 0.09
    assert measurements["gxcopy:count (i)"] == 9688

    for node in graphframe.graph.traverse():
        assert node._depth <= 10


def test_filter_by_min_percentage_of_application_time(data_dir: str) -> None:
    graphframe = GraphFrame.from_hpctoolkit_latest(
        f"{data_dir}/hpctoolkit-gamess", min_percentage_of_application_time=1
    )

    assert len(graphframe.graph.roots) == 1
    assert graphframe.graph.roots[0]._hatchet_nid == 1195
    assert graphframe.graph.roots[0]._depth == 0
    assert graphframe.graph.roots[0].frame["name"] == "entry"
    assert graphframe.graph.roots[0].frame["type"] == "entry"

    assert len(graphframe.dataframe) == 164
    assert "name" in graphframe.dataframe.columns
    assert "realtime (i)" in graphframe.dataframe.columns
    assert "realtime (e)" in graphframe.dataframe.columns
    assert "gpuop (i)" in graphframe.dataframe.columns
    assert "gker (i)" in graphframe.dataframe.columns
    assert "gxcopy (i)" in graphframe.dataframe.columns
    assert "gxcopy:count (i)" in graphframe.dataframe.columns

    measurements = graphframe.dataframe.loc[Node(None, hnid=1195)]
    assert measurements["name"] == "entry"
    assert round(measurements["realtime (i)"], 2) == 1608.49
    assert round(measurements["gpuop (i)"], 2) == 608.09
    assert round(measurements["gker (i)"], 2) == 608.00
    assert round(measurements["gxcopy (i)"], 2) == 0.09
    assert measurements["gxcopy:count (i)"] == 9688
    application_time = measurements["realtime (i)"]

    measurements = graphframe.dataframe.loc[Node(None, hnid=1197)]
    assert measurements["name"] == "gamess_"
    assert round(measurements["realtime (i)"], 2) == 1608.49
    assert round(measurements["gpuop (i)"], 2) == 608.09
    assert round(measurements["gker (i)"], 2) == 608.00
    assert round(measurements["gxcopy (i)"], 2) == 0.09
    assert measurements["gxcopy:count (i)"] == 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=2856)]
    assert measurements["name"] == "__GI___sched_yield"
    assert round(measurements["realtime (i)"], 3) == 159.238
    assert round(measurements["realtime (e)"], 3) == 159.238

    measurements = graphframe.dataframe.loc[Node(None, hnid=251)]
    assert measurements["name"] == "[libc-2.31.so]:0"
    assert round(measurements["realtime (i)"], 3) == 159.238
    assert round(measurements["realtime (e)"], 3) == 159.238

    for node in graphframe.graph.traverse():
        node_time = graphframe.dataframe.loc[node]["realtime (i)"]
        assert node_time / application_time >= 0.01


def test_filter_by_min_percentage_of_parent_time(data_dir: str) -> None:
    graphframe = GraphFrame.from_hpctoolkit_latest(
        f"{data_dir}/hpctoolkit-gamess", min_percentage_of_parent_time=1
    )

    assert len(graphframe.graph.roots) == 1
    assert graphframe.graph.roots[0]._hatchet_nid == 1195
    assert graphframe.graph.roots[0]._depth == 0
    assert graphframe.graph.roots[0].frame["name"] == "entry"
    assert graphframe.graph.roots[0].frame["type"] == "entry"

    assert len(graphframe.dataframe) == 4576
    assert "name" in graphframe.dataframe.columns
    assert "realtime (i)" in graphframe.dataframe.columns
    assert "realtime (e)" in graphframe.dataframe.columns
    assert "gpuop (i)" in graphframe.dataframe.columns
    assert "gker (i)" in graphframe.dataframe.columns
    assert "gxcopy (i)" in graphframe.dataframe.columns
    assert "gxcopy:count (i)" in graphframe.dataframe.columns

    measurements = graphframe.dataframe.loc[Node(None, hnid=1195)]
    assert measurements["name"] == "entry"
    assert round(measurements["realtime (i)"], 2) == 1608.49
    assert round(measurements["gpuop (i)"], 2) == 608.09
    assert round(measurements["gker (i)"], 2) == 608.00
    assert round(measurements["gxcopy (i)"], 2) == 0.09
    assert measurements["gxcopy:count (i)"] == 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=1197)]
    assert measurements["name"] == "gamess_"
    assert round(measurements["realtime (i)"], 2) == 1608.49
    assert round(measurements["gpuop (i)"], 2) == 608.09
    assert round(measurements["gker (i)"], 2) == 608.00
    assert round(measurements["gxcopy (i)"], 2) == 0.09
    assert measurements["gxcopy:count (i)"] == 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=2856)]
    assert measurements["name"] == "__GI___sched_yield"
    assert round(measurements["realtime (i)"], 3) == 159.238
    assert round(measurements["realtime (e)"], 3) == 159.238

    measurements = graphframe.dataframe.loc[Node(None, hnid=251)]
    assert measurements["name"] == "[libc-2.31.so]:0"
    assert round(measurements["realtime (i)"], 3) == 159.238
    assert round(measurements["realtime (e)"], 3) == 159.238

    for node in graphframe.graph.traverse():
        node_time = graphframe.dataframe.loc[node]["realtime (i)"]
        if node.frame["type"] != "entry":
            parent_time = graphframe.dataframe.loc[node.parents[0]]["realtime (i)"]
            assert node_time / parent_time >= 0.01
