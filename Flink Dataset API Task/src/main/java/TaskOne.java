package com.snithish.c;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class NodeShortestPath {
  private final int node;
  private final int source;
  private List<Integer> path;
  private boolean isSource;

  public NodeShortestPath(String idStr, int source) {
    this.node = Integer.parseInt(idStr);
    this.source = source;
    this.path = new ArrayList<>();
    if (source == node) {
      path.add(node);
      isSource = true;
    }
  }

  public boolean isSource() {
    return isSource;
  }

  public int getNode() {
    return node;
  }

  public int getSource() {
    return source;
  }

  public List<Integer> getPath() {
    return path;
  }

  public void setPath(List<Integer> path) {
    this.path = path;
  }

  public int getCost() {
    return path.isEmpty() && !isSource ? Integer.MAX_VALUE : path.size();
  }

  @Override
  public String toString() {
    return "NodeShortestPath{" + "node=" + node + ", path=" + path + ", isSource=" + isSource + '}';
  }
}

class Result {
  public static Tuple4<Integer, Integer, String, String> to(NodeShortestPath nodeShortestPath) {
    int source = nodeShortestPath.getSource();
    int destination = nodeShortestPath.getNode();
    List<Integer> path = nodeShortestPath.getPath();
    if (path.isEmpty()) {
      return Tuple4.of(source, destination, "", "Inf");
    } else {
      String pathStr = path.stream().map(Object::toString).collect(Collectors.joining(", "));
      String cost = String.valueOf(path.size() - 1);
      return Tuple4.of(source, destination, pathStr, cost);
    }
  }
}

public class TaskOne {
  private final int sourceNode;
  private final String inputPath;
  private final String outputPath;

  TaskOne(String sourceNodeId, String inputPath, String outputPath) {
    this.sourceNode = Integer.parseInt(sourceNodeId);
    this.inputPath = inputPath;
    this.outputPath = outputPath;
  }

  void process() throws Exception {

    DataSource<Tuple1<String>> lines = getLines(env);
    DataSet<Tuple2<Integer, NodeShortestPath>> vertices = getVertices(lines);
    DataSet<Tuple2<Integer, Integer>> edges = getEdges(lines);

    DataSet<Tuple2<Integer, NodeShortestPath>> initialWorkSet =
        vertices.filter(new SourceFilter(sourceNode));
    DeltaIteration<Tuple2<Integer, NodeShortestPath>, Tuple2<Integer, NodeShortestPath>>
        activations = vertices.iterateDelta(initialWorkSet, 200, 0);

    DataSet<Tuple3<Integer, List<Integer>, Integer>> messages = getMessages(edges, activations);
    DataSet<Tuple2<Integer, NodeShortestPath>> newActivations =
        getNewActivations(activations, messages);
    activations
        .closeWith(newActivations, newActivations)
        .map(x -> Result.to(x.f1))
        .returns(new TypeHint<Tuple4<Integer, Integer, String, String>>() {})
        .writeAsCsv(outputPath, "\n", "|", FileSystem.WriteMode.OVERWRITE);
    env.execute("Single Source Shortest Path");
    System.out.println("Results are being written to '" + outputPath + "' path.");
  }

  private DataSet<Tuple2<Integer, NodeShortestPath>> getNewActivations(
      DeltaIteration<Tuple2<Integer, NodeShortestPath>, Tuple2<Integer, NodeShortestPath>>
          activations,
      DataSet<Tuple3<Integer, List<Integer>, Integer>> messages) {
    DeltaIteration.SolutionSetPlaceHolder<Tuple2<Integer, NodeShortestPath>> solutionSet =
        activations.getSolutionSet();
    return messages
        .join(solutionSet)
        .where(0)
        .equalTo(0)
        .with(
            new FlatJoinFunction<
                Tuple3<Integer, List<Integer>, Integer>,
                Tuple2<Integer, NodeShortestPath>,
                Tuple2<Integer, NodeShortestPath>>() {
              @Override
              public void join(
                  Tuple3<Integer, List<Integer>, Integer> message,
                  Tuple2<Integer, NodeShortestPath> targetNode,
                  Collector<Tuple2<Integer, NodeShortestPath>> out)
                  throws Exception {
                if (targetNode.f1.getCost() > message.f1.size()) {
                  List<Integer> messagePath = message.f1;
                  messagePath.add(targetNode.f0);
                  targetNode.f1.setPath(messagePath);
                  out.collect(targetNode);
                }
              }
            });
  }

  private DataSet<Tuple3<Integer, List<Integer>, Integer>> getMessages(
      DataSet<Tuple2<Integer, Integer>> edges,
      DeltaIteration<Tuple2<Integer, NodeShortestPath>, Tuple2<Integer, NodeShortestPath>>
          activations)
      throws Exception {
    DataSet<Tuple3<Integer, List<Integer>, Integer>> messages =
        activations
            .getWorkset()
            .join(edges)
            .where(0)
            .equalTo(0)
            .with(
                new FlatJoinFunction<
                    Tuple2<Integer, NodeShortestPath>,
                    Tuple2<Integer, Integer>,
                    Tuple3<Integer, List<Integer>, Integer>>() {
                  @Override
                  public void join(
                      Tuple2<Integer, NodeShortestPath> source,
                      Tuple2<Integer, Integer> edge,
                      Collector<Tuple3<Integer, List<Integer>, Integer>> out)
                      throws Exception {
                    int vertexCost = source.f1.getCost();
                    Integer newCost = vertexCost == Integer.MAX_VALUE ? vertexCost : vertexCost + 1;
                    List<Integer> path = source.f1.getPath();
                    out.collect(Tuple3.of(edge.f1, path, newCost));
                  }
                })
            .groupBy(0)
            .reduce(
                new ReduceFunction<Tuple3<Integer, List<Integer>, Integer>>() {
                  @Override
                  public Tuple3<Integer, List<Integer>, Integer> reduce(
                      Tuple3<Integer, List<Integer>, Integer> first,
                      Tuple3<Integer, List<Integer>, Integer> second)
                      throws Exception {
                    return first.f2 < second.f2 ? first : second;
                  }
                });
    return messages;
  }

  private DataSource<Tuple1<String>> getLines(ExecutionEnvironment env) {
    return env.readCsvFile(inputPath).fieldDelimiter("\\t").ignoreFirstLine().types(String.class);
  }

  private MapOperator<String, Tuple2<Integer, NodeShortestPath>> getVertices(
      DataSource<Tuple1<String>> csvReader) {
    FlatMapOperator<Tuple1<String>, String> tuple1StringFlatMapOperator =
        csvReader.flatMap(
            new FlatMapFunction<Tuple1<String>, String>() {
              @Override
              public void flatMap(Tuple1<String> value, Collector<String> out) throws Exception {
                Arrays.stream(value.f0.split("\\t")).forEach(out::collect);
              }
            });
    return tuple1StringFlatMapOperator.distinct().map(new NodeShortestPathMap(sourceNode));
  }

  private MapOperator<Tuple1<String>, Tuple2<Integer, Integer>> getEdges(
      DataSource<Tuple1<String>> csvReader) {
    return csvReader.map(
        new MapFunction<Tuple1<String>, Tuple2<Integer, Integer>>() {
          @Override
          public Tuple2<Integer, Integer> map(Tuple1<String> value) throws Exception {
            String[] splits = value.f0.split("\\t");
            return Tuple2.of(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]));
          }
        });
  }

  static class SourceFilter implements FilterFunction<Tuple2<Integer, NodeShortestPath>> {
    private final int sourceNode;

    public SourceFilter(int sourceNode) {
      this.sourceNode = sourceNode;
    }

    @Override
    public boolean filter(Tuple2<Integer, NodeShortestPath> value) throws Exception {
      return value.f0 == sourceNode;
    }
  }

  static class NodeShortestPathMap
      implements MapFunction<String, Tuple2<Integer, NodeShortestPath>> {
    private final int source;

    public NodeShortestPathMap(int source) {
      this.source = source;
    }

    @Override
    public Tuple2<Integer, NodeShortestPath> map(String value) throws Exception {
      NodeShortestPath nodeShortestPath = new NodeShortestPath(value, source);
      return Tuple2.of(nodeShortestPath.getNode(), nodeShortestPath);
    }
  }
}
