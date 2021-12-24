package de.tuberlin.dima.aim3.exercises.streaming;

import de.tuberlin.dima.aim3.exercises.model.DebsFeature;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Objects;

/*
 maximum average distance a player runs every five minutes window  of 1 minute sliding window
* */
public class MaxAverageDistance extends ProcessAllWindowFunction<DebsFeature, Tuple3<Long, Long, Double>, TimeWindow>
{
    private transient ValueState<Tuple3<Long, Long, Double>> maxAverageWindow;

     public static Long HIGHEST_TIMESTAMP=15180000L;

    public void process(Context context, Iterable<DebsFeature> iterable, Collector<Tuple3<Long, Long, Double>> collector) throws Exception
       {

        maxAverageWindow = context.globalState().getState(new ValueStateDescriptor<>("Maximum Average Window", TypeInformation.of(new TypeHint<Tuple3<Long, Long, Double>>() {
        })));

        DebsFeature oldEvent = iterable.iterator().next();
        double sum = 0;
        long count = 0;
        for (DebsFeature currentEvent : iterable) {
            count++;
            sum += euclideanDistance(oldEvent, currentEvent);
            oldEvent = currentEvent;
        }
        double distanceAverage = sum / count;

        Tuple3<Long, Long, Double> currentTuple = new Tuple3<>(context.window().getStart(), context.window().getEnd(), distanceAverage);


        if (maxAverageWindow.value()==null || maxAverageWindow.value().f2.compareTo(distanceAverage) < 0)
        {
             maxAverageWindow.update(currentTuple);
        }

        if (currentTuple.f1.compareTo(HIGHEST_TIMESTAMP)>=0)
        {
            collector.collect(new Tuple3<>(maxAverageWindow.value().f0, maxAverageWindow.value().f1, maxAverageWindow.value().f2));
        }
    }

    /*
    Euclidean Distance
     */
    private static double euclideanDistance(DebsFeature previous, DebsFeature current) {
        if (Objects.isNull(previous)) {
            return 0.0;
        }
        double distance =Math.sqrt(Math.pow(current.getPositionX() - previous.getPositionX(), 2) +
                Math.pow(current.getPositionY() - previous.getPositionY(), 2));
        return distance;

    }
}
