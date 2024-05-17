from hatchet import GraphFrame

def main():
    graphframe = GraphFrame.from_hpctoolkit_latest("/g/g91/grbic1/hpc-analysis/test_cases/gamess-1n-siCap-gpu.d/", min_percentage_of_application_time=1)
    #graphframe2 = graphframe + graphframe
    #graphframe = graphframe.filter_out_nodes(max_depth=10)
    print(graphframe.tree())

    #graphframe.collapsable_visualization()

if __name__ == '__main__':
    main()
