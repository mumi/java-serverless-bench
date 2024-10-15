package com.ibm.trl.serverlessbench;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import org.jboss.logging.Logger;
import org.jgrapht.Graph;
import org.jgrapht.generate.BarabasiAlbertGraphGenerator;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.builder.GraphTypeBuilder;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.util.SupplierUtil;

import io.quarkus.funqy.Funq;

public class GraphBFS {
    private static final double nanosecInSec = 1_000_000_000.0;

    static Map<String, Integer> size_generators = Map.of("test",   10,
                                                         "tiny",   100,
                                                         "small",  1000,
                                                         "medium", 10000,
                                                         "large",  100000);

    private static final Logger log = Logger.getLogger(GraphBFS.class);

    public static class FunInput {
        public String size;
        public boolean debug;
    }

    @Funq("graph-bfs")
    @BenchmarkWrapper
    public Map<String, Object> graph_bfs(FunInput input) {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || input.size == null) {
            retVal.put("message", "ERROR: GraphBFS unable to run. size need to be set.");
            return retVal;
        }
        Map<String, Object> measurement = new HashMap<>();

        int graphSize = graphSize(input.size);

        Graph<Integer, DefaultEdge> inputGraph = genGraph(graphSize, measurement);

        var vertices  = new ArrayList<Integer>(graphSize);
        var layers    = new ArrayList<Integer>(graphSize);
        var parents   = new Integer[graphSize];

        long process_begin = System.nanoTime();
        BreadthFirstIterator<Integer, DefaultEdge> it = new BreadthFirstIterator<>(inputGraph);

        int numVisited = 0;
        int lastDepth = -1;

        for( ; it.hasNext(); numVisited++) {
            Integer v = it.next();
            int depth = it.getDepth(v);

            vertices.add(v);
            parents[v] = it.getParent(v);

            if(depth != lastDepth) {
                layers.add(numVisited);
            }
            lastDepth = depth;
        }
        layers.add(numVisited);

        long process_end= System.nanoTime();

        layers.removeIf(Objects::isNull);

        measurement.put("compute_time", (process_end - process_begin) / nanosecInSec);
        retVal.put("measurement", measurement);
        if (input.debug) {
            retVal.put("output", Map.of("vertices", vertices,
                                        "layers", layers,
                                        "parents", new ArrayList<>(Arrays.asList(parents))));
        }

        log.debug("retVal.measurement="+ retVal.get("measurement"));

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
