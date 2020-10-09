package com.sequence.detection.rest.model;

/**
 * Represents a Completion returned through the {@code quick_stats} endpoint
 * @author Andreas Kosmatopoulos
 */
public class Completion
{
    /**
     * The step index of the completed funnel (e.g. in a funnel A->B->C a completion with step index 1 corresponds to a completion of the A->B sub-funnel)
     */
    private int step;
    /**
     * Total count of completions
     */
    private int completions;
    /**
     * Average duration out of all the completions
     */
    private Double averageDuration;
    /**
     * The last occurrence of all the completions
     */
    private String lastCompletedAt;

    /**
     * Constructor
     * @param step The step index
     * @param completions Total count of completions
     * @param averageDuration The average duration
     * @param lastCompletedAt The last occurrence
     */
    public Completion(int step, int completions, Double averageDuration, String lastCompletedAt) {
        this.step = step;
        this.completions = completions;
        this.averageDuration = averageDuration;
        this.lastCompletedAt = lastCompletedAt;
    }

    /**
     * Set step
     * @param step The step index
     */
    public void setStep(int step) { this.step = step; }

    /**
     * Set completions
     * @param completions Total count of completions
     */
    public void setCompletions(int completions) { this.completions = completions; }

    /**
     * Set average duration
     * @param averageDuration The average duration
     */
    public void setAverageDuration(Double averageDuration) { this.averageDuration = averageDuration; }

    /**
     * Set the last occurrence
     * @param lastCompletedAt The last occurrence
     */
    public void setLastCompletedAt(String lastCompletedAt) { this.lastCompletedAt = lastCompletedAt; }

    /**
     * Get step
     * @return The step index
     */
    public int getStep() { return step; }

    /**
     * Get completions
     * @return Total count of completions
     */
    public int getCompletions() { return completions; }

    /**
     * Get average duration
     * @return The average duration
     */
    public Double getAverageDuration() { return averageDuration; }

    /**
     * Get the last occurrence
     * @return The last occurrence
     */
    public String getLastCompletedAt() { return lastCompletedAt; }

    @Override
    public String toString() {
        return "{" +
                "step=" + step +
                ", completions=" + completions +
                ", averageDuration=" + averageDuration +
                ", lastCompletedAt='" + lastCompletedAt + '\'' +
                '}';
    }
}
