package de.tuberlin.dima.aim3.exercises.model;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

/**
 * A simple POJO for DEBS input features.
 * Description of DEBS dataset is given in:
 *
 * @author Imran, Muhammad
 * @see <a href="https://debs.org/grand-challenges/2013/">https://debs.org/grand-challenges/2013/</a>
 * @see <a href="https://dl.acm.org/doi/10.1145/2488222.2488283">https://dl.acm.org/doi/10.1145/2488222.2488283</a>
 */
public class DebsFeature {
    // sensor id
    private long sensorId;
    // timestamp in picoseconds
    private BigInteger timeStamp;
    // sensor coordinate X
    private int positionX;
    // sensor coordinate Y
    private int positionY;
    // sensor coordinate Z
    private int positionZ;
    // velocity
    private int velocity;
    // acceleration
    private int acceleration;
    //  direction vector X
    private int directionVectorX;
    //  direction vector Y
    private int directionVectorY;
    //  direction vector Z
    private int directionVectorZ;
    // + acceleration vector X
    private int accelerationVectorX;
    //  acceleration vector Y
    private int accelerationVectorY;
    //  acceleration vector Z
    private int accelerationVectorZ;

    /**
     * Constructor to instantiate DebFeature object
     *
     * @param builder to build DebFeature instance
     */
    private DebsFeature(Builder builder) {
        this.sensorId = builder.sensorId;
        this.timeStamp = builder.timeStamp;
        this.positionX = builder.positionX;
        this.positionY = builder.positionY;
        this.positionZ = builder.positionZ;
        this.velocity = builder.velocity;
        this.acceleration = builder.acceleration;
        this.directionVectorX = builder.directionVectorX;
        this.directionVectorY = builder.directionVectorY;
        this.directionVectorZ = builder.directionVectorZ;
        this.accelerationVectorX = builder.accelerationVectorX;
        this.accelerationVectorY = builder.accelerationVectorY;
        this.accelerationVectorZ = builder.accelerationVectorZ;
    }

    /**
     * @param line comma-separated line of an event
     * @return DebFeature object
     * @throws Exception
     */
    public static DebsFeature fromString(String line) throws IllegalArgumentException {
        if (line == null || line.equals("")) throw new IllegalArgumentException("Invalid input string.");
        String[] debsElements = line.split(",");
        return new Builder()
                .setSensorId(Integer.parseInt(debsElements[0]))
                .setTimeStamp(new BigInteger(debsElements[1]))
                .setPositionX(Integer.parseInt(debsElements[2]))
                .setPositionY(Integer.parseInt(debsElements[3]))
                .setPositionZ(Integer.parseInt(debsElements[4]))
                .setVelocity(Integer.parseInt(debsElements[5]))
                .setAcceleration(Integer.parseInt(debsElements[6]))
                .setDirectionVectorX(Integer.parseInt(debsElements[7]))
                .setDirectionVectorY(Integer.parseInt(debsElements[8]))
                .setDirectionVectorZ(Integer.parseInt(debsElements[9]))
                .setAccelerationVectorX(Integer.parseInt(debsElements[10]))
                .setAccelerationVectorY(Integer.parseInt(debsElements[11]))
                .setAccelerationVectorZ(Integer.parseInt(debsElements[12]))
                .build();
    }

    /**
     * @return sensor ID
     */
    public long getSensorId() {
        return sensorId;
    }

    /**
     * @return timestamp as {@link BigInteger}
     */
    public BigInteger getTimeStamp() {
        return timeStamp;
    }

    /**
     * @return coordiate x
     */
    public int getPositionX() {
        return positionX;
    }

    /**
     * @return coordiate y
     */
    public int getPositionY() {
        return positionY;
    }

    /**
     * @return coordiate z
     */
    public int getPositionZ() {
        return positionZ;
    }

    /**
     * @return velocity
     */
    public int getVelocity() {
        return velocity;
    }

    /**
     * @return acceleration
     */
    public int getAcceleration() {
        return acceleration;
    }

    /**
     * @return direction vector x
     */
    public int getDirectionVectorX() {
        return directionVectorX;
    }

    /**
     * @return direction vector y
     */
    public int getDirectionVectorY() {
        return directionVectorY;
    }

    /**
     * @return direction vector z
     */
    public int getDirectionVectorZ() {
        return directionVectorZ;
    }

    /**
     * @return acceleration vector x
     */
    public int getAccelerationVectorX() {
        return accelerationVectorX;
    }

    /**
     * @return acceleration vector y
     */
    public int getAccelerationVectorY() {
        return accelerationVectorY;
    }

    /**
     * @return acceleration vector z
     */
    public int getAccelerationVectorZ() {
        return accelerationVectorZ;
    }

    /**
     * @return string representation of a DebFeature object
     */
    @Override
    public String toString() {
        return "DebsFeature{" +
                "sensorId=" + sensorId +
                ", timeStamp=" + timeStamp +
                ", positionX=" + positionX +
                ", positionY=" + positionY +
                ", positionZ=" + positionZ +
                ", velocity=" + velocity +
                ", acceleration=" + acceleration +
                ", directionVectorX=" + directionVectorX +
                ", directionVectorY=" + directionVectorY +
                ", directionVectorZ=" + directionVectorZ +
                ", accelerationVectorX=" + accelerationVectorX +
                ", accelerationVectorY=" + accelerationVectorY +
                ", accelerationVectorZ=" + accelerationVectorZ +
                '}';
    }





    /**
     * <i>builder</i> class to create DebsFeature objects
     */
    public static class Builder {
        private long sensorId;
        private BigInteger timeStamp;
        private int positionX;
        private int positionY;
        private int positionZ;
        private int velocity;
        private int acceleration;
        private int directionVectorX;
        private int directionVectorY;
        private int directionVectorZ;
        private int accelerationVectorX;
        private int accelerationVectorY;
        private int accelerationVectorZ;

        /**
         * default constructor
         */
        public Builder() {
        }

        /**
         * @param sensorId
         * @return Builder to build DebFeature
         */
        public Builder setSensorId(long sensorId) {
            this.sensorId = sensorId;
            return this;
        }

        /**
         * @param timeStamp
         * @return Builder to build DebFeature
         */
        public Builder setTimeStamp(BigInteger timeStamp) {
            this.timeStamp = timeStamp;
            return this;
        }

        /**
         * @param positionX
         * @return Builder to build DebFeature
         */
        public Builder setPositionX(int positionX) {
            this.positionX = positionX;
            return this;
        }

        /**
         * @param positionY
         * @return Builder to build DebFeature
         */
        public Builder setPositionY(int positionY) {
            this.positionY = positionY;
            return this;
        }

        /**
         * @param positionZ
         * @return Builder to build DebFeature
         */
        public Builder setPositionZ(int positionZ) {
            this.positionZ = positionZ;
            return this;
        }

        /**
         * @param velocity
         * @return Builder to build DebFeature
         */
        public Builder setVelocity(int velocity) {
            this.velocity = velocity;
            return this;
        }

        /**
         * @param acceleration
         * @return Builder to build DebFeature
         */
        public Builder setAcceleration(int acceleration) {
            this.acceleration = acceleration;
            return this;
        }

        /**
         * @param directionVectorX
         * @return Builder to build DebFeature
         */
        public Builder setDirectionVectorX(int directionVectorX) {
            this.directionVectorX = directionVectorX;
            return this;
        }

        /**
         * @param directionVectorY
         * @return Builder to build DebFeature
         */
        public Builder setDirectionVectorY(int directionVectorY) {
            this.directionVectorY = directionVectorY;
            return this;
        }

        /**
         * @param directionVectorZ
         * @return Builder to build DebFeature
         */
        public Builder setDirectionVectorZ(int directionVectorZ) {
            this.directionVectorZ = directionVectorZ;
            return this;
        }

        /**
         * @param accelerationVectorX
         * @return Builder to build DebFeature
         */
        public Builder setAccelerationVectorX(int accelerationVectorX) {
            this.accelerationVectorX = accelerationVectorX;
            return this;
        }

        /**
         * @param accelerationVectorY
         * @return Builder to build DebFeature
         */
        public Builder setAccelerationVectorY(int accelerationVectorY) {
            this.accelerationVectorY = accelerationVectorY;
            return this;
        }

        /**
         * @param accelerationVectorZ
         * @return Builder to build DebFeature
         */
        public Builder setAccelerationVectorZ(int accelerationVectorZ) {
            this.accelerationVectorZ = accelerationVectorZ;
            return this;
        }

        /**
         * @return DebFeature instance
         */
        public DebsFeature build() {
            return new DebsFeature(this);
        }
    }
}
