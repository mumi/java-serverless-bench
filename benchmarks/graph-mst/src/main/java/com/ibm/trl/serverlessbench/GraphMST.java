package com.ibm.trl.serverlessbench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import org.jboss.logging.Logger;
import org.jgrapht.Graph;
import org.jgrapht.alg.interfaces.SpanningTreeAlgorithm;
import org.jgrapht.alg.spanning.PrimMinimumSpanningTree;
import org.jgrapht.generate.BarabasiAlbertGraphGenerator;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.builder.GraphTypeBuilder;
import org.jgrapht.util.SupplierUtil;

import io.quarkus.funqy.Funq;

public class GraphMST {
    private static final double nanosecInSec = 1_000_000_000.0;

    static Map<String, Integer> size_generators = Map.of("test",   10,
                                                         "tiny",   100,
                                                         "small",  1000,
                                                         "medium", 10000,
                                                         "large",  100000);

    private final Logger log = Logger.getLogger(GraphMST.class);

    public static class FunInput {
        public String size;
        public boolean debug;
    }

    @Funq("graph-mst")
    @BenchmarkWrapper
    public Map<String, Object> graph_mst(FunInput input) {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || input.size == null) {
            retVal.put("message", "ERROR: GraphMST unable to run. size need to be set.");
            return retVal;
        }
        Map<String, Object> measurement = new HashMap<>();

        int graphSize = graphSize(input.size);

        Graph<Integer, DefaultEdge> inputGraph = genGraph(graphSize, measurement);

        SpanningTreeAlgorithm<DefaultEdge> algo = new PrimMinimumSpanningTree<>(inputGraph);

        long process_begin = System.nanoTime();
        SpanningTreeAlgorithm.SpanningTree<DefaultEdge> mst = algo.getSpanningTree();
        long process_end= System.nanoTime();

        ArrayList<String> mstList = new ArrayList<>(graphSize);
        for (Iterator<DefaultEdge> it = mst.iterator(); it.hasNext(); mstList.add(it.next().toString()));

        measurement.put("compute_time", (process_end - process_begin) / nanosecInSec);
        retVal.put("measurement", measurement);
        if (input.debug) {
            retVal.put("output", Map.of("mst", mstList));
        }

        log.debug("retVal.measurement="+retVal.get("measurement"));

        return retVal;
    }

    private int graphSize(String size) {
        int graphSize = 10;  // default size is "test"

        if(size != null) {
            Integer gs = size_generators.get(size);
            if(gs != null) {
                graphSize = gs;
            } else if(!size.isEmpty()) {
                graphSize = Integer.parseUnsignedInt(size);
            }
        }

        return graphSize;
    }

    private Graph<Integer, DefaultEdge> genGraph(int size, Map<String, Object> measurement) {
        Graph<Integer, DefaultEdge> inputGraph = GraphTypeBuilder.<Integer, DefaultEdge>undirected()
                                                                   .allowingMultipleEdges(true)
                                                                   .edgeClass(DefaultEdge.class)
                                                                   .vertexSupplier(SupplierUtil.createIntegerSupplier())
                                                                   .buildGraph();

        BarabasiAlbertGraphGenerator<Integer, DefaultEdge> generator = 
                new BarabasiAlbertGraphGenerator<>(10, 1, size);

        long graph_generating_begin = System.nanoTime();
        generator.generateGraph(inputGraph);
        long graph_generating_end= System.nanoTime();

        measurement.put("graph_generating_time", (graph_generating_end - graph_generating_begin) / nanosecInSec);

        return inputGraph;
    }
}
