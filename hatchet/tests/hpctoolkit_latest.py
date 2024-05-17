from hatchet import GraphFrame
from hatchet.node import Node


def test_import_entire_db(data_dir: str) -> None:
    graphframe = GraphFrame.from_hpctoolkit_latest(f"{data_dir}/hpctoolkit-gamess")

    assert len(graphframe.graph.roots) == 1
    assert graphframe.graph.roots[0]._hatchet_nid == 1195
    assert graphframe.graph.roots[0].frame["depth"] == 0
    assert graphframe.graph.roots[0].frame["name"] == 1
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
    assert measurements["realtime (i)"] >= 1608.49
    assert measurements["gpuop (i)"] >= 608.09
    assert measurements["gker (i)"] >= 608.00
    assert measurements["gxcopy (i)"] >= 0.09
    assert measurements["gxcopy:count (i)"] >= 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=1197)]
    assert measurements["name"] == "function: gamess_"
    assert measurements["realtime (i)"] >= 1608.49
    assert measurements["gpuop (i)"] >= 608.09
    assert measurements["gker (i)"] >= 608.00
    assert measurements["gxcopy (i)"] >= 0.09
    assert measurements["gxcopy:count (i)"] >= 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=1004)]
    assert measurements["name"] == "loop: [libsci_cray.so.5.0]:0"
    assert measurements["realtime (i)"] >= 0.08
    assert measurements["realtime (e)"] >= 0.08

    measurements = graphframe.dataframe.loc[Node(None, hnid=1003)]
    assert measurements["name"] == "line: [libsci_cray.so.5.0]:0"
    assert measurements["realtime (i)"] >= 0.08
    assert measurements["realtime (e)"] >= 0.08


def test_filter_by_max_depth(data_dir: str) -> None:
    graphframe = GraphFrame.from_hpctoolkit_latest(
        f"{data_dir}/hpctoolkit-gamess", max_depth=10
    )

    assert len(graphframe.graph.roots) == 1
    assert graphframe.graph.roots[0]._hatchet_nid == 1195
    assert graphframe.graph.roots[0].frame["depth"] == 0
    assert graphframe.graph.roots[0].frame["name"] == 1
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
    assert measurements["realtime (i)"] >= 1608.49
    assert measurements["gpuop (i)"] >= 608.09
    assert measurements["gker (i)"] >= 608.00
    assert measurements["gxcopy (i)"] >= 0.09
    assert measurements["gxcopy:count (i)"] >= 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=1197)]
    assert measurements["name"] == "function: gamess_"
    assert measurements["realtime (i)"] >= 1608.49
    assert measurements["gpuop (i)"] >= 608.09
    assert measurements["gker (i)"] >= 608.00
    assert measurements["gxcopy (i)"] >= 0.09
    assert measurements["gxcopy:count (i)"] >= 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=9846)]
    assert measurements["name"] == "function: wfn_"
    assert measurements["realtime (i)"] >= 786.09
    assert measurements["gpuop (i)"] >= 608.09
    assert measurements["gker (i)"] >= 608.00
    assert measurements["gxcopy (i)"] >= 0.09
    assert measurements["gxcopy:count (i)"] >= 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=9845)]
    assert measurements["name"] == "loop: [gamess.00.x]:0"
    assert measurements["realtime (i)"] >= 786.09
    assert measurements["gpuop (i)"] >= 608.09
    assert measurements["gker (i)"] >= 608.00
    assert measurements["gxcopy (i)"] >= 0.09
    assert measurements["gxcopy:count (i)"] >= 9688


def test_filter_by_min_percentage_of_application_time(data_dir: str) -> None:
    graphframe = GraphFrame.from_hpctoolkit_latest(
        f"{data_dir}/hpctoolkit-gamess", min_percentage_of_application_time=1
    )

    assert len(graphframe.graph.roots) == 1
    assert graphframe.graph.roots[0]._hatchet_nid == 1195
    assert graphframe.graph.roots[0].frame["depth"] == 0
    assert graphframe.graph.roots[0].frame["name"] == 1
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
    assert measurements["realtime (i)"] >= 1608.49
    assert measurements["gpuop (i)"] >= 608.09
    assert measurements["gker (i)"] >= 608.00
    assert measurements["gxcopy (i)"] >= 0.09
    assert measurements["gxcopy:count (i)"] >= 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=1197)]
    assert measurements["name"] == "function: gamess_"
    assert measurements["realtime (i)"] >= 1608.49
    assert measurements["gpuop (i)"] >= 608.09
    assert measurements["gker (i)"] >= 608.00
    assert measurements["gxcopy (i)"] >= 0.09
    assert measurements["gxcopy:count (i)"] >= 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=2856)]
    assert measurements["name"] == "function: __GI___sched_yield [libc-2.31.so]"
    assert measurements["realtime (i)"] >= 159.23
    assert measurements["realtime (e)"] >= 159.23

    measurements = graphframe.dataframe.loc[Node(None, hnid=251)]
    assert measurements["name"] == "line: [libc-2.31.so]:0"
    assert measurements["realtime (i)"] >= 159.23
    assert measurements["realtime (e)"] >= 159.23


def test_filter_by_min_percentage_of_parent_time(data_dir: str) -> None:
    graphframe = GraphFrame.from_hpctoolkit_latest(
        f"{data_dir}/hpctoolkit-gamess", min_percentage_of_parent_time=1
    )

    assert len(graphframe.graph.roots) == 1
    assert graphframe.graph.roots[0]._hatchet_nid == 1195
    assert graphframe.graph.roots[0].frame["depth"] == 0
    assert graphframe.graph.roots[0].frame["name"] == 1
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
    assert measurements["realtime (i)"] >= 1608.49
    assert measurements["gpuop (i)"] >= 608.09
    assert measurements["gker (i)"] >= 608.00
    assert measurements["gxcopy (i)"] >= 0.09
    assert measurements["gxcopy:count (i)"] >= 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=1197)]
    assert measurements["name"] == "function: gamess_"
    assert measurements["realtime (i)"] >= 1608.49
    assert measurements["gpuop (i)"] >= 608.09
    assert measurements["gker (i)"] >= 608.00
    assert measurements["gxcopy (i)"] >= 0.09
    assert measurements["gxcopy:count (i)"] >= 9688

    measurements = graphframe.dataframe.loc[Node(None, hnid=2856)]
    assert measurements["name"] == "function: __GI___sched_yield [libc-2.31.so]"
    assert measurements["realtime (i)"] >= 159.23
    assert measurements["realtime (e)"] >= 159.23

    measurements = graphframe.dataframe.loc[Node(None, hnid=251)]
    assert measurements["name"] == "line: [libc-2.31.so]:0"
    assert measurements["realtime (i)"] >= 159.23
    assert measurements["realtime (e)"] >= 159.23
