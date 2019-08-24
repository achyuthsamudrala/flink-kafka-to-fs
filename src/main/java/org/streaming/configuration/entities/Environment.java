package org.streaming.configuration.entities;

public class Environment {

    private int sourceParallelism;
    private int sinkParallelism;
    private long checkpointingInterval;
    private String checkpointsDir;
    private String outputDir;

    public Environment() {
    }

    public int getSourceParallelism() {
        return sourceParallelism;
    }

    public void setSourceParallelism(int sourceParallelism) {
        this.sourceParallelism = sourceParallelism;
    }

    public int getSinkParallelism() {
        return sinkParallelism;
    }

    public void setSinkParallelism(int sinkParallelism) {
        this.sinkParallelism = sinkParallelism;
    }


    public long getCheckpointingInterval() {
        return checkpointingInterval;
    }

    public void setCheckpointingInterval(long checkpointingInterval) {
        this.checkpointingInterval = checkpointingInterval;
    }

    public String getCheckpointsDir() {
        return checkpointsDir;
    }

    public void setCheckpointsDir(String checkpointsDir) {
        this.checkpointsDir = checkpointsDir;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }


    @Override
    public String toString() {
        return "Environment{" +
                " sourceParallelism=" + sourceParallelism +
                " sinkParallelism=" + sinkParallelism +
                " checkpointingInterval=" + checkpointingInterval +
                " checkpointsDir=" + checkpointsDir +
                '}';
    }
}
