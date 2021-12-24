package de.tuberlin.dima.aim3.exercises.streaming;

import de.tuberlin.dima.aim3.exercises.model.DebsFeature;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The implementation of {@link FootballStatistics} to perform analytics on DEBS dataset.
 *
 * @author Imran, Muhammad
 */
public class FootballStatisticsImpl implements FootballStatistics {
    /*
        The StreamExecutionEnvironment is the context in which a streaming program is executed.
    */
    final StreamExecutionEnvironment STREAM_EXECUTION_ENVIRONMENT =  StreamExecutionEnvironment.getExecutionEnvironment();
    /*
        File path of the dataset.
    */
    private final String filePath;
    /**
     * stream of events to be evaluated lazily
     */
    private DataStream<DebsFeature> events;

    /**
     * @param filePath dataset file path as a {@link String}
     */
    FootballStatisticsImpl(String filePath) {
        this.filePath = filePath;
    }

    /**
     * write the events that show that ball almost came near goal (within penalty area),
     * but the ball was kicked out of the penalty area by a player of the opposite team.
     */


    @Override
    public void writeAvertedGoalEvents() {
        events = events.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<DebsFeature>() {

            @Override
            public long extractAscendingTimestamp(DebsFeature element) {
                return element.getTimeStamp().divide(new BigInteger(String.valueOf(1000000000))).longValue();
            }
        });

        List<Long> SensorIds =Arrays.asList(4L, 8L, 10L, 12L);

        events.filter(x->SensorIds.contains(x.getSensorId()))
                .keyBy(DebsFeature::getSensorId)
                .timeWindowAll(Time.minutes(1))
                .process(new ProcessAllWindowFunction<DebsFeature, Tuple2<String, Integer>, TimeWindow>() {

                   /*
                   Ball inside Team A penality area
                   */
                    private boolean isBallInsideTeamAPenaltyArea(DebsFeature feature) {
                        int x = feature.getPositionX();
                        int y = feature.getPositionY();
                        return (x > 6300 && x < 46183 && y > 15940 && y < 33940);
                    }
                    /*
                 Ball inside Team B penality area
                 */
                    private boolean isBallInsideTeamBPenaltyArea(DebsFeature feature) {
                        int x = feature.getPositionX();
                        int y = feature.getPositionY();
                        return (x > 6300 && x < 46183 && y > -33968 && y < -15965);
                    }
                    /*
                            for every 1 minutes get averteg goals of both teams
                     */
                    public void process(
                            Context context,
                            Iterable<DebsFeature> elements,
                            Collector<Tuple2<String, Integer>> out) {
                        DebsFeature previous = null;
                        int teamACounter = 0;
                        int teamBCounter = 0;

                        for (DebsFeature current : elements) {
                            if (Objects.isNull(previous)) {
                                previous = current;
                                continue;
                            }
                            /*
                            current should be outside and previous should be inside:goal is not scored
                            */
                            if (!isBallInsideTeamAPenaltyArea(current) && isBallInsideTeamAPenaltyArea(previous)) {
                                teamACounter++;
                            } else if (!isBallInsideTeamBPenaltyArea(current) && isBallInsideTeamBPenaltyArea(previous)) {
                                teamBCounter++;
                            }
                            previous = current;
                        }
                        out.collect(new Tuple2<>("TeamA",teamACounter));
                        out.collect(new Tuple2<>("TeamB", teamBCounter));
                    }

                })
                // for the gloabal window for summing
                .timeWindowAll(Time.hours(10))
                .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {

                    /*
                    global sum of all averted goals of both teams
                     */

                    public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) {

                        int teamACounter = 0;
                        int teamBCounter = 0;
                        for (Tuple2<String, Integer> element : elements) {
                            String key = element.f0;
                            if (key.equalsIgnoreCase("TeamA")) {
                                teamACounter+=element.f1;
                            } else {
                                teamBCounter+=element.f1;
                            }
                        }
                        out.collect(new Tuple2<>("TeamA", teamACounter));
                        out.collect(new Tuple2<>("TeamB", teamBCounter));
                    }
                })
                .writeAsCsv("Data/avertedGoals.csv",FileSystem.WriteMode.OVERWRITE, "\n",",")
                .setParallelism(1);

                System.out.println("Compute the total number of events over the entire game for which the team almost scored a goal");
        try {
            STREAM_EXECUTION_ENVIRONMENT.execute("writeAvertedGoalEvents");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * Highest average distance of player A1 (of Team A) ran in every 5 minutes duration. You can skip 1 minute duration between every two durations.
     */
    @Override
    public void writeHighestAvgDistanceCovered() {
        // TODO: Write your code here.
        events = events.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<DebsFeature>() {

            @Override
            public long extractAscendingTimestamp(DebsFeature element) {
                return element.getTimeStamp().divide(new BigInteger(String.valueOf(1000000000))).longValue();

            }
        });
        events.filter(t -> t.getSensorId() == 47)
                .timeWindowAll(Time.minutes(5), Time.minutes(1))
                .process(new MaxAverageDistance())
                .writeAsCsv("Data/highestAvg.csv", FileSystem.WriteMode.OVERWRITE, "\n", ",")
                .setParallelism(1);

            System.out.println("Computing maximum average distance run  by player A1 in a 5 minute window, where the duration between consecutive windows is 1 minute");

            try {
                STREAM_EXECUTION_ENVIRONMENT.execute("writeHighestAvgDistanceCovered");
            }

            catch (Exception ex)
            {
                ex.printStackTrace();
            }

    }


    /**
     * Creates {@link StreamExecutionEnvironment} and {@link DataStream} before each streaming task
     * to initialize a stream from the beginning.
     */
    @Override
    public void initStreamExecEnv() {
        
         /*
          Setting the default parallelism of our execution environment.
          Feel free to change it according to your environment/number of operators, sources, and sinks you would use.
          However, it should not have any impact on your results whatsoever.
         */
        STREAM_EXECUTION_ENVIRONMENT.setParallelism(2);

        /*
          Event time is the time that each individual event occurred on its producing device.
          This time is typically embedded within the records before they enter Flink.
         */
        STREAM_EXECUTION_ENVIRONMENT.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
          Reads the file as a text file.
         */
        DataStream<String> dataStream = STREAM_EXECUTION_ENVIRONMENT.readTextFile(filePath);

        /*
          Creates DebsFeature for each record.
          ALTERNATIVELY You can use Tuple type (For that you would need to change generic type of 'event').
         */
        events = dataStream.map(DebsFeature::fromString);
    }
}
