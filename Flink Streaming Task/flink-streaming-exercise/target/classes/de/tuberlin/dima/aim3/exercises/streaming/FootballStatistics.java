package de.tuberlin.dima.aim3.exercises.streaming;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Imran, Muhammad
 */
public interface FootballStatistics {

    /**
     * @param filePath the path to the data file as {@link String}
     * @return An instance of a subclass of {@link FootballStatistics}
     * @throws IllegalAccessException if the path is null
     */
    static FootballStatistics newInstance(String filePath) throws IllegalAccessException {
        if (filePath == null) {
            throw new IllegalAccessException("The provided file path is invalid.");
        }
        return new FootballStatisticsImpl(filePath);
    }

    /**
     * write the events that show that the football almost came near goal (within the penalty area),
     * but the football was kicked out of the penalty area by a player of opposite team
     */
    void writeAvertedGoalEvents();

    /**
     * Highest average distance of player A1 (of Team A) ran in every 5 minutes duration. You can skip 1 minute duration between every two durations.
     */
    void writeHighestAvgDistanceCovered();

    /**
     * Creates {@link StreamExecutionEnvironment} and {@link DataStream} before each streaming task
     * to initialize a stream from the beginning.
     */
    void initStreamExecEnv();
}

