package org.dissertation.producer;

public record RateObservation(String name, double startSeconds, double endSeconds,
        double targetAverageRatePerSecond, long sent, long acknowledged,
        double observedRatePerSecond, Double rateErrorPercent) {
}
